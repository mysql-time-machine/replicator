package com.booking.replication.applier;

import com.booking.replication.augmenter.AugmentedRow;
import com.booking.replication.augmenter.AugmentedRowsEvent;
import com.booking.replication.Metrics;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static com.codahale.metrics.MetricRegistry.name;

public class HBaseApplierWriter {

    /**
     * batch-transaction-based buffer:
     *
     * Buffer is structured by tasks. Each task can have multiple transactions, each transaction can have multiple
     * tables and each table can have multiple mutations. Each task is identified by task UUID. Each transaction is
     * identified with transaction UUID. Task sub-buffers are picked up by flusher threads and on success there
     * identified with transaction UUID. Task sub-buffers are picked up by flusher threads and on success there
     * are two options:
     *
     *      1. the task UUID key is deleted from the the buffer if all transactions are marked for commit.
     *
     *      2. If there is a transactions not marked for commit (large transactions, so buffer is full before
     *         end of transaction is reached), the new task UUID is created and the transaction UUID of the
     *         unfinished transaction is reserved in the new task-sub-buffer.
     *
     * On task failure, task status is updated to 'WRITE_FAILED' and that task will be retried. The hash structure
     * of single task sub-buffer looks like this:
     *
     *  {
     *      "874c3466-3bf0-422f-a3e3-148289226b6c" => { // <- transaction UUID
     *
     *        table_1 => [@table_1_augmented_row_changes]
     *        ,...,
     *        table_N => [@table_N_augmented_row_changes]
     *
     *      },
     *
     *      "187433e5-7b05-47ff-a3bd-633897cd2b4f" => {
     *
     *        table_1 => [@table_1_augmented_row_changes]
     *        ,...,
     *        table_N => [@table_N_augmented_row_changes]
     *
     *      },
     *  }
     *
     * Or in short, Perl-like syntax:
     *
     *  $taskBuffer = { $taskUUID => { $transactionUUID => { $tableName => [@AugmentedRows] }}}
     *
     * This works asynchronously for maximum performance. Since transactions are timestamped and they are from RBR
     * we can buffer them in any order. In HBase all of them will be present with corresponding timestamp. And RBR
     * guaranties that each operation is idempotent (so there is no queries that transform data like update value
     * to value * x, which would break the idempotent feature of operations). Simply put, the order of applying of
     * different transactions does not influence the end result since data will be timestamped with timestamps
     * from the binlog and if there are multiple operations on the same row all versions are kept in HBase.
     */
    private final
            ConcurrentHashMap<String, Map<String, Map<String,List<AugmentedRow>>>>
            taskTransactionBuffer = new ConcurrentHashMap<>();

    /**
     * Futures grouped by task UUID
     */
    private final
            ConcurrentHashMap<String, Future<TaskResult>>
            taskFutures = new ConcurrentHashMap<>();

    /**
     * Status tracking helper structures
     */
    private final ConcurrentHashMap<String, Integer>      taskStatus        = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Integer>      transactionStatus = new ConcurrentHashMap<>();

    /**
     * Shared connection used by all tasks in applier
     */
    private Connection hbaseConnection;

    /**
     * Task thread pool
     */
    private static ExecutorService taskPool;

    // TODO: add to startup options
    private final int POOL_SIZE;

    // dry run option; TODO: add to startup options
    private final boolean DRY_RUN = false;

    private static volatile String currentTaskUUID;
    private static volatile String currentTransactionUUID;

    // rowsBufferedInCurrentTask is the size of currentTaskUUID buffer. Once this buffer
    // is full, it is submitted and new one is opened with new taskUUID
    public AtomicInteger rowsBufferedInCurrentTask = new AtomicInteger(0);

    private final Configuration hbaseConf;

    private static final Logger LOGGER = LoggerFactory.getLogger(HBaseApplierWriter.class);

    private final  com.booking.replication.Configuration configuration;

    private static final Counter
            applierTasksSubmittedCounter = Metrics.registry.counter(name("HBase", "applierTasksSubmittedCounter"));
    private static final Counter
            applierTasksSucceededCounter = Metrics.registry.counter(name("HBase", "applierTasksSucceededCounter"));
    private static final Counter
            applierTasksFailedCounter = Metrics.registry.counter(name("HBase", "applierTasksFailedCounter"));

    //@todo: the logic here is sufficient but not exhaustive, improve robustness of following code
    public boolean areAllTasksDone() {
        for(String key: taskStatus.keySet()) {
            if(taskStatus.get(key) != TaskStatusCatalog.WRITE_SUCCEEDED) {
                return false;
            }
        }
        return true;
    }

    // ================================================
    // Constructor
    // ================================================
    public HBaseApplierWriter(
            int poolSize,
            org.apache.hadoop.conf.Configuration hbaseConfiguration,
            com.booking.replication.Configuration repCfg
        ) {

        POOL_SIZE         = poolSize;
        taskPool          = Executors.newFixedThreadPool(POOL_SIZE);

        hbaseConf         = hbaseConfiguration;

        configuration     = repCfg;

        if (! DRY_RUN) {
            try {
                hbaseConnection = ConnectionFactory.createConnection(hbaseConf);
            } catch (IOException e) {
                LOGGER.error("Failed to create hbase connection", e);
            }
        }

        initBuffers();

        Metrics.registry.register(name("HBase", "hbaseWriterTaskQueueSize"),
                new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return taskQueueSize;
                    }
                });

        Metrics.registry.register(name("HBase", "hbaseWriterSlotWaitTime"),
                new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        if (slotWaitTime == 0) {
                            return 0L;
                        }
                        return System.currentTimeMillis() - slotWaitTime;
                    }
                });
    }

    public void initBuffers() {

        currentTaskUUID = UUID.randomUUID().toString();
        currentTransactionUUID = UUID.randomUUID().toString();

        taskTransactionBuffer
                .put(currentTaskUUID, new HashMap<String, Map<String, List<AugmentedRow>>>());
        taskTransactionBuffer.get(currentTaskUUID)
                .put(currentTransactionUUID, new HashMap<String, List<AugmentedRow>>());

        taskStatus.put(currentTaskUUID, TaskStatusCatalog.READY_FOR_BUFFERING);
        transactionStatus.put(currentTransactionUUID, TransactionStatus.OPEN);
    }

    // ================================================
    // Buffering util
    // ================================================
    public synchronized void pushToCurrentTaskBuffer(AugmentedRowsEvent augmentedRowsEvent) {

        //String             hbaseTableName = hBasePreparedAugmentedRowsEvent.getHbaseTableName();
        String             mySQLTableName = augmentedRowsEvent.getMysqlTableName();
        List<AugmentedRow> augmentedRows  = augmentedRowsEvent.getSingleRowEvents();

        // Verify that task_uuid exists
        if (taskTransactionBuffer.get(currentTaskUUID) == null) {
            LOGGER.error("ERROR: Missing task UUID from taskTransactionBuffer keySet should not happen. " +
                    "Shutting down...");
            System.exit(1);
        }

        // Verify that transaction_uuid exists
        if (taskTransactionBuffer.get(currentTaskUUID).get(currentTransactionUUID) == null) {
            LOGGER.error("ERROR: Missing transaction UUID from taskTransactionBuffer keySet should not happen. " +
                    "Shutting down...");
            System.exit(1);
        }

        // Verify that table exists. If not, add to transaction. In case of delta
        // tables, delta table key will belong to the same task and transaction
        // as corresponding mirrored table
        if (taskTransactionBuffer.get(currentTaskUUID).get(currentTransactionUUID).get(mySQLTableName) == null) {
            taskTransactionBuffer
                    .get(currentTaskUUID)
                    .get(currentTransactionUUID)
                    .put(mySQLTableName, new ArrayList<AugmentedRow>());
        }

        // Add to buffer
        for(AugmentedRow augmentedRow : augmentedRows) {
            taskTransactionBuffer
                    .get(currentTaskUUID)
                    .get(currentTransactionUUID)
                    .get(mySQLTableName)
                    .add(augmentedRow);
            rowsBufferedInCurrentTask.incrementAndGet();
        }
    }

    // ================================================
    // Flushing util
    // ================================================
    public void markCurrentTransactionForCommit() {

        // mark
        transactionStatus.put(currentTransactionUUID, TransactionStatus.READY_FOR_COMMIT);

        // open a new transaction slot and set it as the current transaction
        currentTransactionUUID = UUID.randomUUID().toString();
        taskTransactionBuffer.get(currentTaskUUID).put(currentTransactionUUID, new HashMap<String,List<AugmentedRow>>());
        transactionStatus.put(currentTransactionUUID, TransactionStatus.OPEN);
    }

    public void markCurrentTaskAsReadyAndCreateNewUUIDBuffer() {
        // don't create new buffers if no slots available
        blockIfNoSlotsAvailableForBuffering();

        // mark current uuid buffer as READY_FOR_PICK_UP unless there are no
        // rows buffered (then just keep the buffer ready for next binlog file)
        if (rowsBufferedInCurrentTask.get() > 0) {
            taskStatus.put(currentTaskUUID, TaskStatusCatalog.READY_FOR_PICK_UP);
        } else {
            return;
        }

        // create new uuid buffer
        String newTaskUUID = UUID.randomUUID().toString();

        taskTransactionBuffer.put(newTaskUUID, new HashMap<String, Map<String, List<AugmentedRow>>>());

        // Check if there is an open/unfinished transaction in current UUID task buffer and
        // if so, create/reserve the corresponding transaction UUID in the new UUID task buffer
        // so that the transaction rows that are on the way can be buffered under the same UUID.
        // This is a foundation for the TODO: when XID event is received and the end of transaction tie
        // the transaction id from XID with the transaction UUID used for buffering. The goal is
        // to be able to identify mutations in HBase which were part of the same transaction.
        int openTransactions = 0;
        for (String transactionUUID : taskTransactionBuffer.get(currentTaskUUID).keySet()) {
            if (transactionStatus.get(transactionUUID) == TransactionStatus.OPEN) {
                openTransactions++;
                if (openTransactions > 1) {
                    LOGGER.error("More than one partial transaction in the buffer. Should never happen! Exiting...");
                    System.exit(-1);
                }
                taskTransactionBuffer.get(newTaskUUID).put(transactionUUID, new HashMap<String,List<AugmentedRow>>() );
                currentTransactionUUID = transactionUUID; // <- important
            }
        }

        taskStatus.put(newTaskUUID, TaskStatusCatalog.READY_FOR_BUFFERING);

        currentTaskUUID = newTaskUUID;

        rowsBufferedInCurrentTask.set(0);

        // update task queue size
        long queueSize = 0;
        for (String taskUUID : taskStatus.keySet()) {
            if (taskStatus.get(taskUUID) == TaskStatusCatalog.READY_FOR_PICK_UP) {
                queueSize++;
            }
        }
        taskQueueSize = queueSize;
    }

    private Long taskQueueSize = 0L;

    public void markAllTasksAsReadyToGo() {

        // mark current uuid buffer as READY_FOR_PICK_UP
        int numberOfTasksLeft = taskStatus.keySet().size();

        LOGGER.info("Tasks left: " + numberOfTasksLeft);

        if (numberOfTasksLeft != 0) {
            for (String taskUUID : taskStatus.keySet()) {

                if (taskStatus.get(taskUUID) == TaskStatusCatalog.WRITE_IN_PROGRESS) {
                    LOGGER.info("task " + taskUUID + " => " + "WRITE_IN_PROGRESS");
                } else if (taskStatus.get(taskUUID) == TaskStatusCatalog.WRITE_FAILED) {
                    LOGGER.info("task " + taskUUID + " => " + "WRITE_FAILED");
                } else if (taskStatus.get(taskUUID) == TaskStatusCatalog.TASK_SUBMITTED) {
                    LOGGER.info("task " + taskUUID + " => " + "TASK_SUBMITTED");
                } else if (taskStatus.get(taskUUID) == TaskStatusCatalog.READY_FOR_PICK_UP) {
                    LOGGER.info("task " + taskUUID + " => " + "READY_FOR_PICK_UP");
                } else if (taskStatus.get(taskUUID) == TaskStatusCatalog.READY_FOR_BUFFERING) {
                    LOGGER.info("task " + taskUUID + " => " + "READY_FOR_BUFFERING");
                    if (taskHasRowsBuffered(taskUUID)) {
                        taskStatus.put(taskUUID, TaskStatusCatalog.READY_FOR_PICK_UP);
                        LOGGER.info("Marked task " + taskUUID + " as READY_FOR_PICK_UP");
                    } else {
                        // cant flush empty task
                        taskStatus.remove(taskUUID);
                        taskTransactionBuffer.remove(taskUUID);
                        if (taskFutures.containsKey(taskUUID)) {
                            taskFutures.remove(taskUUID);
                        }
                    }
                } else if (taskStatus.get(taskUUID) == TaskStatusCatalog.WRITE_SUCCEEDED) {
                    LOGGER.info("task " + taskUUID + " => " + "WRITE_SUCCEEDED");
                } else {
                    LOGGER.info("task " + taskUUID + " => " + "UNKNOWN STATUS => " + taskStatus.get(taskUUID));
                }
            }
        } else {
            taskQueueSize = 0L;
        }
    }

    private long slotWaitTime = 0L;

    private void blockIfNoSlotsAvailableForBuffering() {

        boolean block = true;
        int blockingTime = 0;

        slotWaitTime = System.currentTimeMillis();
        while (block) {

            updateTaskStatuses();

            int currentNumberOfTasks = taskTransactionBuffer.keySet().size();

            if (currentNumberOfTasks > POOL_SIZE) {
                try {
                    Thread.sleep(5);
                    blockingTime += 5;
                } catch (InterruptedException e) {
                    LOGGER.error("Cant sleep.", e);
                }
                if ((blockingTime % 500) == 0) {
                    LOGGER.warn("Too many tasks already open ( " + currentNumberOfTasks + " ), blocking time is " + blockingTime + "ms");
                }
                if (blockingTime > 60000) {
                    LOGGER.error("Timed out waiting for an open applier slot after 60s.");
                    throw new RuntimeException("Timed out waiting on applier slot");
                }
            } else {
                if(blockingTime > 1000) {
                    LOGGER.warn("Wait is over with " + currentNumberOfTasks + " current tasks, blocking time was " + blockingTime + "ms");
                }

                slotWaitTime = 0;
                block = false;
            }
        }
    }

    public void updateTaskStatuses() {

        // clean up and re-queue failed tasks
        Set<String> taskFuturesUUIDs = taskFutures.keySet();

        // Loop submitted tasks
        for (String submittedTaskUUID : taskFuturesUUIDs) {

            Future<TaskResult> taskFuture;

            try {

                taskFuture = taskFutures.get(submittedTaskUUID);

                // Process done tasks
                if (taskFuture.isDone()) {

                    LOGGER.info("Task " + submittedTaskUUID + " is done");

                    TaskResult taskResult = taskFuture.get(); // raise exceptions if any
                    boolean taskSucceeded = taskResult.isTaskSucceeded();

                    int statusOfDoneTask = taskResult.getTaskStatus();

                    if (statusOfDoneTask == TaskStatusCatalog.WRITE_SUCCEEDED) {
                        if (!taskSucceeded) {
                            throw new Exception("Inconsistent success reports for task " + submittedTaskUUID);
                        }

                        applierTasksSucceededCounter.inc();

                        taskStatus.remove(submittedTaskUUID);

                        // the following two are structured by task-transaction, so
                        // if there is an open transaction UUID in this task, it has
                        // already been copied to the new/next task
                        taskTransactionBuffer.remove(submittedTaskUUID);

                        // since the task is done, remove the key from the futures hash
                        taskFutures.remove(submittedTaskUUID);


                    } else if (statusOfDoneTask == TaskStatusCatalog.WRITE_FAILED) {
                        if (taskSucceeded) {
                            throw new Exception("Inconsistent success reports for task " + submittedTaskUUID);
                        }
                        LOGGER.warn("Task " + submittedTaskUUID + " failed. Task will be retried.");
                        requeueTask(submittedTaskUUID);
                        applierTasksFailedCounter.inc();
                    } else {
                        LOGGER.error("Illegal task status ["
                                + statusOfDoneTask
                                + "]. Probably a silent death of a thread. "
                                + "Will consider the task as failed and re-queue.");
                        requeueTask(submittedTaskUUID);
                        applierTasksFailedCounter.inc();
                    }
                }
            } catch (ExecutionException ex) {
                LOGGER.error(String.format("Future failed for task %s, with exception: %s",
                        submittedTaskUUID ,
                        ex.getCause().toString()));
                requeueTask(submittedTaskUUID);
                applierTasksFailedCounter.inc();
            } catch (InterruptedException ei) {
                LOGGER.info(String.format("Task %s was canceled by interrupt. " +
                        "The task that has been canceled " +
                        "will be retired later by another future.", submittedTaskUUID), ei);
                requeueTask(submittedTaskUUID);
                applierTasksFailedCounter.inc();
            } catch (Exception e) {
                LOGGER.error("Inconsistent success reports for task " + submittedTaskUUID + ". Will retry the task.");
                requeueTask(submittedTaskUUID);
                applierTasksFailedCounter.inc();
            }
        }
    }

    /**
     * requeueTask
     *
     * @param failedTaskUUID UUID
     */
    private void requeueTask(String failedTaskUUID) {

        // remove the key from the futures hash; new future will be created
        taskFutures.remove(failedTaskUUID);

        // keep the mutation buffer, just change the status so this task is picked up again
        taskStatus.put(failedTaskUUID, TaskStatusCatalog.READY_FOR_PICK_UP);
    }

    private boolean taskHasRowsBuffered(String taskUUID) {

        boolean taskHasRows = false;

        Map<String, Map<String, List<AugmentedRow>>> task = taskTransactionBuffer.get(taskUUID);

        for (String transactionUUID : task.keySet()) {
            Set<String> transactionTables = task.get(transactionUUID).keySet();
            for (String tableName : transactionTables) {
                List<AugmentedRow> bufferedOPS = task.get(transactionUUID).get(tableName);
                if (bufferedOPS != null && bufferedOPS.size() > 0) {
                    taskHasRows = true;
                } else {
                    LOGGER.info("Table " + tableName + " has no rows!!!");
                }
            }
        }

        return taskHasRows;
    }

    /**
     * Submit tasks that are READY_FOR_PICK_UP
     */
    public void submitTasksThatAreReadyForPickUp() {

        if ((! DRY_RUN) && (hbaseConnection == null)) {
            LOGGER.info("HBase connection is gone. Will try to recreate new connection...");
            int retry = 10;
            while (retry > 0) {
                try {
                    hbaseConnection = ConnectionFactory.createConnection(hbaseConf);
                    retry = 0;
                } catch (IOException e) {
                    LOGGER.warn("Failed to create hbase connection from HBaseApplier, attempt " + retry + "/10");
                }
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    LOGGER.error("Thread wont sleep. Not a good day for you.",e);
                }
                retry--;
            }
        }

        if ((! DRY_RUN) && (hbaseConnection == null)) {
            LOGGER.error("Could not create HBase connection, all retry attempts failed. Exiting...");
            System.exit(-1);
        }

        // one future per task
        for (final String taskUUID : taskStatus.keySet()) {

            boolean taskHasRows = false;

            Map<String, Map<String, List<AugmentedRow>>> task = taskTransactionBuffer.get(taskUUID);
            if (task == null) {
                throw new RuntimeException(String.format("Task %s is null", taskUUID));
            }

            for (String transactionUUID : task.keySet()) {
                Set<String> transactionTables = task.get(transactionUUID).keySet();
                for (String tableName : transactionTables) {
                    List<AugmentedRow> bufferedOPS = task.get(transactionUUID).get(tableName);
                    if (bufferedOPS != null && bufferedOPS.size() > 0) {
                        taskHasRows = true;
                    } else {
                        LOGGER.info("Table " + tableName + " has no rows!!!");
                    }
                }
            }

            // submit task
            if ((taskStatus.get(taskUUID) == TaskStatusCatalog.READY_FOR_PICK_UP)) {
                if (taskHasRows) {

                    LOGGER.info("Submitting task " + taskUUID);

                    taskStatus.put(taskUUID, TaskStatusCatalog.TASK_SUBMITTED);

                    applierTasksSubmittedCounter.inc();

                    taskFutures.put(taskUUID, taskPool.submit(
                        new HBaseWriterTask(
                                configuration,
                                hbaseConnection,
                                taskUUID,
                                taskTransactionBuffer.get(taskUUID)
                        )
                    ));
                } else {
                    LOGGER.error("Task is marked as READY_FOR_PICK_UP, but has no rows. Exiting...");
                    System.exit(1);
                }
            }
        }
    }
}
