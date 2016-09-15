package com.booking.replication.applier.hbase;

import static com.codahale.metrics.MetricRegistry.name;

import com.booking.replication.Metrics;
import com.booking.replication.applier.ChaosMonkey;
import com.booking.replication.applier.TaskStatus;
import com.booking.replication.augmenter.AugmentedRow;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Triple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.Callable;

public class HBaseWriterTask implements Callable<HBaseTaskResult> {

    private static final Logger LOGGER = LoggerFactory.getLogger(HBaseWriterTask.class);

    private static boolean DRY_RUN;

    private static final Counter applierTasksInProgressCounter = Metrics.registry.counter(name("HBase", "applierTasksInProgressCounter"));
    private static final Meter rowOpsCommittedToHbase = Metrics.registry.meter(name("HBase", "rowOpsCommittedToHbase"));
    private static final Timer putLatencyTimer = Metrics.registry.timer(name("HBase", "writerPutLatency"));
    private static final Timer taskLatencyTimer = Metrics.registry.timer(name("HBase", "writerTaskLatency"));

    private final Connection hbaseConnection;
    private final HBaseApplierMutationGenerator mutationGenerator;
    private final String taskUuid;
    private final Map<String, TransactionProxy> taskTransactionBuffer;

    /**
     * Parallelised worker that generates and applies HBase mutations.
     *
     * @param conn          Connection to HBase cluster
     * @param generator     HBase Mutation Generator
     * @param id            Our task id
     * @param taskBuffer    Our task buffer
     */
    public HBaseWriterTask(
            Connection conn,
            HBaseApplierMutationGenerator generator,
            String id,
            Map<String, TransactionProxy> taskBuffer,
            boolean dryRun
    ) {
        super();
        
        DRY_RUN = dryRun;

        hbaseConnection = conn;
        taskUuid = id;
        mutationGenerator = generator;
        taskTransactionBuffer = taskBuffer;
    }

    @Override
    public HBaseTaskResult call() throws Exception {

        final Timer.Context taskTimer = taskLatencyTimer.time();

        ChaosMonkey chaosMonkey = new ChaosMonkey();

        if (chaosMonkey.feelsLikeThrowingExceptionAfterTaskSubmitted()) {
            throw new Exception("Chaos monkey exception for submitted task!");
        }

        if (chaosMonkey.feelsLikeFailingSubmitedTaskWithoutException()) {
            return new HBaseTaskResult(taskUuid, TaskStatus.WRITE_FAILED, false);
        }

        applierTasksInProgressCounter.inc();

        if (chaosMonkey.feelsLikeThrowingExceptionForTaskInProgress()) {
            throw new Exception("Chaos monkey exception for task in progress!");
        }
        if (chaosMonkey.feelsLikeFailingTaskInProgessWithoutException()) {
            return new HBaseTaskResult(taskUuid, TaskStatus.WRITE_FAILED, false);
        }

        for (final String transactionUuid : taskTransactionBuffer.keySet()) {

            int numberOfTablesInCurrentTransaction = taskTransactionBuffer.get(transactionUuid).keySet().size();

            int numberOfFlushedTablesInCurrentTransaction = 0;

            final Timer.Context timerContext = putLatencyTimer.time();
            for (final String bufferedMySQLTableName : taskTransactionBuffer.get(transactionUuid).keySet()) {

                if (chaosMonkey.feelsLikeThrowingExceptionBeforeFlushingData()) {
                    throw new Exception("Chaos monkey is here to prevent call to flush!!!");
                } else if (chaosMonkey.feelsLikeFailingDataFlushWithoutException()) {
                    return new HBaseTaskResult(taskUuid, TaskStatus.WRITE_FAILED, false);
                } else {
                    List<AugmentedRow> rowOps = taskTransactionBuffer.get(transactionUuid).get(bufferedMySQLTableName);

                    HashMap<String, HashMap<String, List<Triple<String, String, Put>>>> preparedMutations =
                            mutationGenerator.generateMutationsFromAugmentedRows(rowOps);

                    if (!preparedMutations.containsKey("mirrored")) {
                        LOGGER.error("Missing mirrored key from preparedMutations!");
                        System.exit(-1);
                    }

                    for (String type: "mirrored delta".split(" ")) {

                        for (String hbaseTableName : preparedMutations.get(type).keySet()) {

                            List<Put> puts = new ArrayList<>();

                            for (Triple<String, String, Put> augmentedMutation :
                                    preparedMutations.get(type).get(hbaseTableName)) {
                                puts.add(augmentedMutation.getThird());
                            }

                            if (!DRY_RUN) {
                                TableName tableName = TableName.valueOf(hbaseTableName);
                                Table hbaseTable = hbaseConnection.getTable(tableName);
                                hbaseTable.put(puts);
                            } else {
                                System.out.println("Running in dry-run mode, prepared " + puts.size() + " mutations.");
                                Thread.sleep(1000);
                            }

                            if (type.equals("mirrored")) {
                                numberOfFlushedTablesInCurrentTransaction++;
                            }

                            PerTableMetrics.get(hbaseTableName).committed.inc(puts.size());

                            rowOpsCommittedToHbase.mark(puts.size());
                        }
                    }
                }
            } // next table
            timerContext.stop();

            // data integrity check
            if (numberOfTablesInCurrentTransaction != numberOfFlushedTablesInCurrentTransaction) {
                LOGGER.error(String.format("Failed integrity check number of tables: %s != %s",
                        numberOfTablesInCurrentTransaction,
                        numberOfFlushedTablesInCurrentTransaction));
                return new HBaseTaskResult(taskUuid, TaskStatus.WRITE_FAILED, false);
            }
        } // next transaction

        taskTimer.stop();

        if (DRY_RUN) {
            Thread.sleep(100);
            return new HBaseTaskResult(
                    taskUuid,
                    TaskStatus.WRITE_SUCCEEDED,
                    true
            );
        }

        // task result
        return new HBaseTaskResult(
                taskUuid,
                TaskStatus.WRITE_SUCCEEDED,
                true
        );
    }

    private static class PerTableMetrics {
        private static String prefix = "HBase";
        private static final HashMap<String, PerTableMetrics> tableMetricsHash = new HashMap<>();

        static PerTableMetrics get(String tableName) {
            synchronized (tableMetricsHash) {
                if (! tableMetricsHash.containsKey(tableName)) {
                    tableMetricsHash.put(tableName, new PerTableMetrics(tableName));
                }
                return tableMetricsHash.get(tableName);
            }
        }

        final Counter committed;

        PerTableMetrics(String tableName) {
            committed   = Metrics.registry.counter(name(prefix, tableName, "committed"));
        }
    }
}
