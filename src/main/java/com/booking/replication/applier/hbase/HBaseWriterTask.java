package com.booking.replication.applier.hbase;

import static com.codahale.metrics.MetricRegistry.name;

import com.booking.replication.Metrics;
import com.booking.replication.applier.ChaosMonkey;
import com.booking.replication.applier.TaskStatus;
import com.booking.replication.augmenter.AugmentedRow;
import com.booking.replication.util.CaseInsensitiveMap;
import com.booking.replication.validation.ValidationService;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

public class HBaseWriterTask implements Callable<HBaseTaskResult> {

    private static final Logger LOGGER = LoggerFactory.getLogger(HBaseWriterTask.class);

    private static boolean DRY_RUN;

    private static final Counter applierTasksInProgressCounter = Metrics.registry.counter(name("HBase", "applierTasksInProgressCounter"));
    private static final Meter rowOpsCommittedToHbase = Metrics.registry.meter(name("HBase", "rowOpsCommittedToHbase"));
    private static final Timer putLatencyTimer = Metrics.registry.timer(name("HBase", "writerPutLatency"));
    private static final Timer taskLatencyTimer = Metrics.registry.timer(name("HBase", "writerTaskLatency"));

    private final ValidationService validationService;

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
            ValidationService validationService,
            boolean dryRun
    ) {
        super();
        
        DRY_RUN = dryRun;

        hbaseConnection = conn;
        taskUuid = id;
        mutationGenerator = generator;
        taskTransactionBuffer = taskBuffer;
        this.validationService = validationService;
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

                    Map<String, List<HBaseApplierMutationGenerator.PutMutation>> mutationsByTable = mutationGenerator.generateMutations(rowOps).stream()
                            .collect(
                                        Collectors.groupingBy( mutation->mutation.getTable()
                                    )
                            );

                    for (Map.Entry<String, List<HBaseApplierMutationGenerator.PutMutation>> entry : mutationsByTable.entrySet()){

                        String tableName = entry.getKey();
                        List<HBaseApplierMutationGenerator.PutMutation> mutations = entry.getValue();

                        if (!DRY_RUN) {
                            Table table = hbaseConnection.getTable(TableName.valueOf(tableName));
                            table.put( mutations.stream().map( mutation -> mutation.getPut() ).collect(Collectors.toList()) );
                            table.close();

                            for (HBaseApplierMutationGenerator.PutMutation mutation : mutations){
                                if (validationService != null) validationService.registerValidationTask(transactionUuid, mutation.getSourceRowUri(), mutation.getTargetRowUri());
                            }

                        } else {
                            System.out.println("Running in dry-run mode, prepared " + mutations.size() + " mutations.");
                            Thread.sleep(1000);
                        }

                        if (entry.getValue().get(0).isTableMirrored()) {
                            numberOfFlushedTablesInCurrentTransaction++;
                        }

                        PerTableMetrics.get(tableName).committed.inc(mutations.size());

                        rowOpsCommittedToHbase.mark(mutations.size());

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
        private static final Map<String, PerTableMetrics> tableMetricsHash = new CaseInsensitiveMap<>();

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
