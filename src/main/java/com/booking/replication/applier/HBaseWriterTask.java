package com.booking.replication.applier;

import com.booking.replication.Configuration;
import com.booking.replication.Metrics;
import com.booking.replication.augmenter.AugmentedRow;
import com.booking.replication.util.MutableLong;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Triple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;

import static com.codahale.metrics.MetricRegistry.name;

public class HBaseWriterTask implements Callable<TaskResult> {

    private static final Logger LOGGER = LoggerFactory.getLogger(HBaseWriterTask.class);

    private static final boolean DRY_RUN = false;

    private String taskUUID;

    private static final Counter applierTasksInProgressCounter = Metrics.registry.counter(name("HBase", "applierTasksInProgressCounter"));
    private static final Meter rowOpsCommittedToHbase = Metrics.registry.meter(name("HBase", "rowOpsCommittedToHbase"));

    private static final Metrics.PerTableMetricsHash perHBaseTableCounters = new Metrics.PerTableMetricsHash("HBase");

    private Configuration configuration;
    private Connection hbaseConnection;

    public HBaseWriterTask(
            Configuration config,
            Connection conn,
            String id,
            Map<String, Map<String, List<String>>> taskRows,
            Map<String, Map<String, List<AugmentedRow>>> taskBuffer
    ) {
        super();
        configuration = config;
        hbaseConnection = conn;
        taskUUID = id;
        taskRowIDS = taskRows;
        taskTransactionBuffer = taskBuffer;
    }

    /**
     * helper buffer of the same structure for row-ids only
     */
    private Map<String, Map<String,List<String>>>
            taskRowIDS = new ConcurrentHashMap<>();

    private Map<String, Map<String,List<AugmentedRow>>>
            taskTransactionBuffer = new ConcurrentHashMap<>();

    @Override
    public TaskResult call() throws Exception {
        try {
            long numberOfMySQLRowsInTask = 0;

            // for unique rows tracking
            HashMap<String, HashMap<String, HashMap<String, MutableLong>>> hbaseRowsAffectedPerTable = new HashMap<>();

            HashMap<String, HashMap<String, MutableLong>> taskStatsPerTable = new HashMap<>();

            for (String transactionUUID : taskRowIDS.keySet()) {

                for (String tableName : taskRowIDS.get(transactionUUID).keySet()) {

                    List<String> bufferedMySQLIDs = taskRowIDS.get(transactionUUID).get(tableName);

                    long numberOfBufferedMySQLIDsForTable = bufferedMySQLIDs.size();

                    if (taskStatsPerTable.get("mysql") == null) {
                        taskStatsPerTable.put("mysql", new HashMap<String, MutableLong>());
                        taskStatsPerTable.get("mysql").put(tableName, new MutableLong(numberOfBufferedMySQLIDsForTable));
                    } else if (taskStatsPerTable.get("mysql").get(tableName) == null) {
                        taskStatsPerTable.get("mysql").put(tableName, new MutableLong(numberOfBufferedMySQLIDsForTable));
                    } else {
                        taskStatsPerTable.get("mysql").get(tableName).addValue(numberOfBufferedMySQLIDsForTable);
                    }
                    numberOfMySQLRowsInTask += numberOfBufferedMySQLIDsForTable;
                }
            }

            LOGGER.info("Metric of rows in task " + taskUUID + " => " + numberOfMySQLRowsInTask);

            ChaosMonkey chaosMonkey = new ChaosMonkey();

            if (chaosMonkey.feelsLikeThrowingExceptionAfterTaskSubmitted()) {
                throw new Exception("Chaos monkey exception for submitted task!");
            }

            int taskStatus;

            if (chaosMonkey.feelsLikeFailingSubmitedTaskWithoutException()) {
                return new TaskResult(taskUUID, TaskStatusCatalog.WRITE_FAILED, false);
            }

            applierTasksInProgressCounter.inc();

            if (chaosMonkey.feelsLikeThrowingExceptionForTaskInProgress()) {
                throw new Exception("Chaos monkey exception for task in progress!");
            }
            if (chaosMonkey.feelsLikeFailingTaskInProgessWithoutException()) {
                return new TaskResult(taskUUID, TaskStatusCatalog.WRITE_FAILED, false);
            }

            for (final String transactionUUID : taskTransactionBuffer.keySet()) {

                int numberOfTablesInCurrentTransaction = taskTransactionBuffer.get(transactionUUID).keySet().size();

                int numberOfFlushedTablesInCurrentTransaction = 0;

                for (final String bufferedMySQLTableName : taskTransactionBuffer.get(transactionUUID).keySet()) {

                    if (chaosMonkey.feelsLikeThrowingExceptionBeforeFlushingData()) {
                        throw new Exception("Chaos monkey is here to prevent call to flush!!!");
                    } else if (chaosMonkey.feelsLikeFailingDataFlushWithoutException()) {
                        return new TaskResult(taskUUID, TaskStatusCatalog.WRITE_FAILED, false);
                    } else {
                        List<AugmentedRow> rowOps = taskTransactionBuffer.get(transactionUUID).get(bufferedMySQLTableName);

                        HBaseApplierMutationGenerator hBaseApplierMutationGenerator =
                                new HBaseApplierMutationGenerator(configuration);

                        HashMap<String, HashMap<String, List<Triple<String, String, Put>>>> preparedMutations =
                                hBaseApplierMutationGenerator.generateMutationsFromAugmentedRows(rowOps);

                        if(!preparedMutations.containsKey("mirrored")) {
                            LOGGER.error("Missing mirrored key from preparedMutations!");
                            System.exit(-1);
                        }

                        for (String type: "mirrored delta".split(" ")) {

                            if(!hbaseRowsAffectedPerTable.containsKey(type)) {
                                hbaseRowsAffectedPerTable.put(type, new HashMap<String, HashMap<String, MutableLong>>());
                            }

                            HashMap<String, HashMap<String, MutableLong>> rowsAffected = hbaseRowsAffectedPerTable.get(type);

                            for (String HBaseTableName : preparedMutations.get(type).keySet()) {

                                List<Put> puts = new ArrayList<>();

                                for (Triple<String, String, Put> augmentedMutation : preparedMutations.get(type).get(HBaseTableName)) {

                                    puts.add(augmentedMutation.getThird());

                                    if (rowsAffected.get(HBaseTableName) == null) {
                                        rowsAffected.put(HBaseTableName, new HashMap<String, MutableLong>());
                                    }

                                    if (rowsAffected.get(HBaseTableName).get(augmentedMutation.getSecond()) == null) {
                                        rowsAffected.get(HBaseTableName).put(augmentedMutation.getSecond(), new MutableLong(1L));
                                    } else {
                                        rowsAffected.get(HBaseTableName).get(augmentedMutation.getSecond()).addValue(1L);
                                    }
                                }

                                if (!DRY_RUN) {
                                    TableName TABLE = TableName.valueOf(HBaseTableName);
                                    Table hbaseTable = hbaseConnection.getTable(TABLE);
                                    hbaseTable.put(puts);
                                }

                                if(type.equals("mirrored")) {
                                    numberOfFlushedTablesInCurrentTransaction++;
                                }

                                Metrics.PerTableMetrics tableMetrics = perHBaseTableCounters.getOrCreate(HBaseTableName);
                                for (String rowKey : hbaseRowsAffectedPerTable.get(type).get(HBaseTableName).keySet()) {
                                    tableMetrics.committed.inc(hbaseRowsAffectedPerTable.get(type).get(HBaseTableName).get(rowKey).getValue());
                                    rowOpsCommittedToHbase.mark(hbaseRowsAffectedPerTable.get(type).get(HBaseTableName).get(rowKey).getValue());
                                }
                            }
                        }
                    }
                } // next table

                // data integrity check
                if (numberOfTablesInCurrentTransaction != numberOfFlushedTablesInCurrentTransaction) {
                    LOGGER.error(String.format("Failed integrity check number of tables: %s != %s", numberOfTablesInCurrentTransaction, numberOfFlushedTablesInCurrentTransaction));
                    return new TaskResult(taskUUID, TaskStatusCatalog.WRITE_FAILED, false);
                }
            } // next transaction

            taskStatus = TaskStatusCatalog.WRITE_SUCCEEDED;

            // task result
            return new TaskResult(
                    taskUUID,
                    taskStatus,
                    true
            );
        } catch (NullPointerException e) {
            LOGGER.error("NullPointerException in future", e);
            e.printStackTrace();
            System.exit(1);
        }
        return null;
    }
}
