package com.booking.replication.applier.hbase;

import com.booking.replication.Metrics;
import com.booking.replication.applier.ApplierException;
import com.booking.replication.applier.TaskStatus;
import com.booking.replication.augmenter.AugmentedRow;
import com.booking.replication.augmenter.AugmentedRowsEvent;
import com.booking.replication.checkpoints.LastCommittedPositionCheckpoint;
import com.booking.replication.validation.ValidationService;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static com.codahale.metrics.MetricRegistry.name;

public class HBaseApplierWriter {

    /**
     * Batch Transaction buffer.
     *
     * <p>Buffer is structured by tasks. Each task can have multiple transactions, each transaction can have multiple
     * tables and each table can have multiple mutations. Each task is identified by task UUID. Each transaction is
     * identified with transaction UUID. Task sub-buffers are picked up by flusher threads and on success there
     * identified with transaction UUID. Task sub-buffers are picked up by flusher threads and on success there
     * are two options:
     *
     *      1. the task UUID key is deleted from the the buffer if all transactions are marked for commit.
     *
     *      2. If there is a transactions not marked for commit (large transactions, so buffer is full before
     *         end of transaction is reached), the new task UUID is created and the transaction UUID of the
     *         unfinished transaction is reserved in the new task-sub-buffer.</p>
     *
     * <p>On task failure, task status is updated to 'WRITE_FAILED' and that task will be retried. The hash structure
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
     *  }</p>
     *
     * <p>Or in short, Perl-like syntax:
     *
     *  $taskBuffer = { $taskUUID => { $transactionUUID => { $tableName => [@AugmentedRows] }}}
     *
     * This works asynchronously for maximum performance. Since transactions are timestamped and they are from RBR
     * we can buffer them in any order. In HBase all of them will be present with corresponding timestamp. And RBR
     * guaranties that each operation is idempotent (so there is no queries that transform data like update value
     * to value * x, which would break the idempotent feature of operations). Simply put, the order of applying of
     * different transactions does not influence the end result since data will be timestamped with timestamps
     * from the binlog and if there are multiple operations on the same row all versions are kept in HBase.</p>
     */
    private final
        ConcurrentHashMap<String, ApplierTask>
        taskTransactionBuffer = new ConcurrentHashMap<>();

    private final
        HBaseApplierNotYetCommittedAccounting
        notYetCommittedTasksAccountant = new HBaseApplierNotYetCommittedAccounting();

    private static final
        ConcurrentHashMap<String, String> taskUUIDToPseudoGTID = new ConcurrentHashMap<>();

    private static LastCommittedPositionCheckpoint latestCommittedPseudoGTIDCheckPoint;
    private final RowTimestampOrganizer timestampOrganizer;
    /**
     * Shared connection used by all tasks in applier.
     */
    private Connection hbaseConnection;

    /**
     * HBase mutation generator.
     */
    private final HBaseApplierMutationGenerator mutationGenerator;

    /**
     * Task thread pool.
     */
    private static ExecutorService taskPool;

    // TODO: add to startup options
    private final int poolSize;

    private static boolean DRY_RUN;

    private static final long MAX_BLOCKING_TIME = 60000; // 1 min
    private static final long MAX_BLOCKING_RETRIES = 3;

    private static volatile String currentTaskUuid = UUID.randomUUID().toString();
    private static volatile String currentTransactionUUID = UUID.randomUUID().toString();

    // rowsBufferedInCurrentTask is the size of currentTaskUuid buffer. Once this buffer
    // is full, it is submitted and new one is opened with new taskUUID
    public AtomicInteger rowsBufferedInCurrentTask = new AtomicInteger(0);

    private static final Logger LOGGER = LoggerFactory.getLogger(HBaseApplierWriter.class);

    private final Configuration hbaseConf = HBaseConfiguration.create();

    private final ValidationService validationService;

    private static final Counter
            applierTasksSubmittedCounter = Metrics.registry.counter(name("HBase", "applierTasksSubmittedCounter"));
    private final Counter applierTasksSucceededCounter;
    private static final Counter
            applierTasksFailedCounter = Metrics.registry.counter(name("HBase", "applierTasksFailedCounter"));

    public static LastCommittedPositionCheckpoint getLatestCommittedPseudoGTIDCheckPoint() {
        return latestCommittedPseudoGTIDCheckPoint;
    }

    /**
     * Helper function to identify if any tasks are still pending.
     *
     * @return will return <code>true</code> only when all tasks have a success status.
     */
    // TODO: the logic here is sufficient but not exhaustive, improve robustness of following code
    public boolean areAllTasksDone() {
        int notFinished = 0;
        for (ApplierTask v: taskTransactionBuffer.values()) {
            if (v.getTaskStatus() != TaskStatus.READY_FOR_BUFFERING
                && v.getTaskStatus() != TaskStatus.WRITE_SUCCEEDED) {
                notFinished++;
            }
        }
        LOGGER.debug("We have " + notFinished + " unfinished tasks.");

        return notFinished == 0;
    }

    /**
     * HBase Applier writer class.
     *
     * <p>The writer manages the worker pool and task status.</p>
     *
     * @param poolSize Size of the worker pool
     * @param configuration Replication configuration object
     */
    public HBaseApplierWriter(
            int poolSize,
            com.booking.replication.Configuration configuration,
            Counter tasksSucceededCounter,
            ValidationService validationService
    ) {
        DRY_RUN = configuration.isDryRunMode();

        this.validationService = validationService;

        this.poolSize = poolSize;
        taskPool          = Executors.newFixedThreadPool(this.poolSize);

        mutationGenerator = new HBaseApplierMutationGenerator(configuration);
        timestampOrganizer = new RowTimestampOrganizer();

        hbaseConf.set("hbase.zookeeper.quorum", configuration.getHBaseQuorum());
        hbaseConf.set("hbase.client.keyvalue.maxsize", "0");

        if (! DRY_RUN) {
            try {
                hbaseConnection = createHBaseConnection();
            } catch (IOException e) {
                LOGGER.error("Failed to create hbase connection. Continuing without it. Exception:", e);
            }
        }

        applierTasksSucceededCounter = tasksSucceededCounter;

        taskTransactionBuffer
                .put(currentTaskUuid, new ApplierTask(TaskStatus.READY_FOR_BUFFERING));
        taskTransactionBuffer.get(currentTaskUuid)
                .put(currentTransactionUUID, new TransactionProxy());

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

        Metrics.registry.register(name("HBase", "transactionBufferSize"),
                new Gauge<Integer>() {
                    @Override
                    public Integer getValue() {
                        return taskTransactionBuffer.size();
                    }
                });
    }

    public Connection createHBaseConnection() throws IOException {
        return createHBaseConnection(3);
    }

    public Connection createHBaseConnection(int retry) throws IOException {
        IOException lastException = null;
        while (retry > 0) {
            try {
                return ConnectionFactory.createConnection(hbaseConf);
            } catch (IOException e) {
                lastException = e;
                LOGGER.warn("Failed to create hbase connection from HBaseApplier, attempt " + retry + "/10");
            }
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                LOGGER.error("Thread wont sleep. Not a good day for you.", e);
            }
            retry--;
        }
        LOGGER.error("Failed to create hbase connection from HBaseApplier, attempt " + retry + "/10. Giving up. Last expcetion was:" + lastException);
        throw new IOException("Could not create HBase connection, all retry attempts failed. Last exception was:", lastException);
    }

    public synchronized void markCurrentTaskWithPseudoGTID(LastCommittedPositionCheckpoint pseudoGTIDCheckPoint)
        throws TaskBufferInconsistencyException {
        // Verify that task uuid exists
        if (taskTransactionBuffer.get(currentTaskUuid) == null) {
            throw new TaskBufferInconsistencyException("ERROR: Missing task UUID ("
                    + currentTaskUuid
                    + ") from taskTransactionBuffer keySet should not happen. ");
        }
        taskTransactionBuffer.get(currentTaskUuid).setPseudoGTIDCheckPoint(pseudoGTIDCheckPoint);
    }

    /**
     * Buffer current event for processing.
     *
     * @param augmentedRowsEvent Event
     * @throws TaskBufferInconsistencyException if task uuid doesn't exist
     */
    public synchronized void pushToCurrentTaskBuffer(AugmentedRowsEvent augmentedRowsEvent)
        throws TaskBufferInconsistencyException {
        // Verify that task uuid exists
        if (taskTransactionBuffer.get(currentTaskUuid) == null) {
            throw new TaskBufferInconsistencyException("ERROR: Missing task UUID ("
                    + currentTaskUuid
                    + ") from taskTransactionBuffer keySet should not happen. ");
        }
        // Verify that transaction_uuid exists
        if (taskTransactionBuffer.get(currentTaskUuid).get(currentTransactionUUID) == null) {

            throw  new TaskBufferInconsistencyException("ERROR: Missing transaction UUID from taskTransactionBuffer keySet!");
        }

        String mySQLTableName = augmentedRowsEvent.getMysqlTableName();

        // Verify that table exists. If not, add to transaction. In case of delta
        // tables, delta table key will belong to the same task and transaction
        // as corresponding mirrored table
        if (taskTransactionBuffer.get(currentTaskUuid).get(currentTransactionUUID).get(mySQLTableName) == null) {
            taskTransactionBuffer
                    .get(currentTaskUuid)
                    .get(currentTransactionUUID)
                    .put(mySQLTableName, new ArrayList<AugmentedRow>());
        }

        List<AugmentedRow> augmentedRows  = augmentedRowsEvent.getSingleRowEvents();
        timestampOrganizer.organizeTimestamps(augmentedRows, mySQLTableName, currentTransactionUUID);

        // Add to buffer
        for (AugmentedRow augmentedRow : augmentedRows) {
            taskTransactionBuffer
                    .get(currentTaskUuid)
                    .get(currentTransactionUUID)
                    .get(mySQLTableName)
                    .add(augmentedRow);
            rowsBufferedInCurrentTask.incrementAndGet();
        }
    }

    /**
     * Flushing utility function.
     */
    public void markCurrentTransactionForCommit() {
        // mark
        taskTransactionBuffer.get(currentTaskUuid).get(currentTransactionUUID).setReadyForCommit();

        // open a new transaction slot and set it as the current transaction
        currentTransactionUUID = UUID.randomUUID().toString();
        taskTransactionBuffer.get(currentTaskUuid).put(currentTransactionUUID, new TransactionProxy());
    }

    /**
     * Rotate tasks, mark current task as ready to be submitted and initialize new task buffer.
     * @throws TaskBufferInconsistencyException More than one partial transaction in the buffer
     * @throws ApplierException No slots available
     * @throws IOException Cannot apply task after maximum retries
     */
    public void markCurrentTaskAsReadyAndCreateNewUuidBuffer()
            throws TaskBufferInconsistencyException, ApplierException, IOException {
        // don't create new buffers if no slots available
        blockIfNoSlotsAvailableForBuffering();

        // mark current uuid buffer as READY_FOR_PICK_UP unless there are no
        // rows buffered (then just keep the buffer ready for next binlog file)
        if (rowsBufferedInCurrentTask.get() > 0) {
            taskTransactionBuffer.get(currentTaskUuid).setTaskStatus(TaskStatus.READY_FOR_PICK_UP);
        } else {
            return;
        }

        // create new uuid buffer
        String newTaskUuid = UUID.randomUUID().toString();

        taskTransactionBuffer.put(newTaskUuid, new ApplierTask(TaskStatus.READY_FOR_BUFFERING));

        // Check if there is an open/unfinished transaction in current UUID task buffer and
        // if so, create/reserve the corresponding transaction UUID in the new UUID task buffer
        // so that the transaction rows that are on the way can be buffered under the same UUID.
        // This is a foundation for the TODO: when XID event is received and the end of transaction tie
        // the transaction id from XID with the transaction UUID used for buffering. The goal is
        // to be able to identify mutations in HBase which were part of the same transaction.
        int openTransactions = 0;
        for (String transactionUuid : taskTransactionBuffer.get(currentTaskUuid).keySet()) {
            if (!taskTransactionBuffer.get(currentTaskUuid).get(transactionUuid).isReadyForCommit()) {
                openTransactions++;
                if (openTransactions > 1) {
                    throw new TaskBufferInconsistencyException("More than one partial transaction in the buffer. Should never happen!");
                }
                taskTransactionBuffer.get(newTaskUuid).put(transactionUuid, new TransactionProxy() );
                currentTransactionUUID = transactionUuid; // <- important
            }
        }

        currentTaskUuid = newTaskUuid;
        LOGGER.debug("Set new currentTaskUuid to: " + currentTaskUuid);

        rowsBufferedInCurrentTask.set(0);

        // update task queue size
        long queueSize = 0;
        for (ApplierTask v : taskTransactionBuffer.values()) {
            if (v.getTaskStatus() == TaskStatus.READY_FOR_PICK_UP) {
                queueSize++;
            }
        }
        taskQueueSize = queueSize;
    }

    private Long taskQueueSize = 0L;

    private long slotWaitTime = 0L;

    private void blockIfNoSlotsAvailableForBuffering() throws ApplierException, IOException {

        boolean block = true;
        int blockingTime = 0;
        int retry = 0;

        slotWaitTime = System.currentTimeMillis();
        while (block && retry < MAX_BLOCKING_RETRIES) {
            try {
                submitTasksThatAreReadyForPickUp();
            } catch (TaskBufferInconsistencyException te) {
                throw new ApplierException(te);
            }
            updateTaskStatuses();

            int currentNumberOfTasks = taskTransactionBuffer.keySet().size();

            if (currentNumberOfTasks > poolSize) {
                try {
                    Thread.sleep(5);
                    blockingTime += 5;
                } catch (InterruptedException e) {
                    LOGGER.error("Cant sleep.", e);
                }

                if (blockingTime >= MAX_BLOCKING_TIME) {
                    LOGGER.warn(String.format("Waiting for an applier slot for %dms which is more than max %dms. Canceling futures", blockingTime, MAX_BLOCKING_TIME));
                    for (String tr: taskTransactionBuffer.keySet()) {
                        LOGGER.warn(String.format("Task %s, rows: %s, status: %s, future: %s",
                                 tr,
                                 taskRowsBuffered(tr),
                                 taskTransactionBuffer.get(tr).getTaskStatus(),
                                 taskTransactionBuffer.get(tr).getTaskFuture()
                             )
                         );
                         if (taskTransactionBuffer.get(tr).getTaskFuture() != null) {
                             taskTransactionBuffer.get(tr).getTaskFuture().cancel(true);
                         } 
                    }
                    blockingTime = 0;
                    retry++;
                }
            } else {
                if (blockingTime > 10000) {
                    LOGGER.info("Wait is over with " + currentNumberOfTasks + " current tasks, blocking time was " + blockingTime + "ms");
                }
                slotWaitTime = 0;
                block = false;
            }
        }
        if (block) {
            String message = String.format("Can't apply tasks after %d retries %dms each", MAX_BLOCKING_RETRIES, MAX_BLOCKING_TIME);
            LOGGER.error(message);
            for (String tr: taskTransactionBuffer.keySet()) {
                LOGGER.error(String.format("Task %s, rows: %s, status: %s, future: %s",
                        tr,
                        taskRowsBuffered(tr),
                        taskTransactionBuffer.get(tr).getTaskStatus(),
                        taskTransactionBuffer.get(tr).getTaskFuture()
                        )
                );
            }
            throw new IOException(message);
        }
    }

    /**
     * Clean up task statuses, requeue tasks where necessary.
     * @throws ApplierException Task accounting exception
     */
    public synchronized void updateTaskStatuses() throws ApplierException {
        // Loop submitted tasks

        for (String submittedTaskUuid : taskTransactionBuffer.keySet()) {

            if (taskTransactionBuffer.get(submittedTaskUuid) == null) {
                // TODO: this is ugly, find a better way to solve this problem
                // skip non-existing key: after accounting for WRITE_SUCCEEDED, there
                // is a chance that some tasks (basically previous tasks in the binlog
                // order that have (all) also been committed) have been removed from
                // taskTransactionBuffer.
                // Since the binlog order is not the same as the order of this loop and
                // since the loop loads the keys once at the beginning, there is a chance
                // to hit a key(s) that has been removed and get a NPE. So, after accounting
                // some keys in the loop may be missing so we need to check for them and skip
                // the loop block for each of them.
                LOGGER.debug("Key " + submittedTaskUuid + " is gone from the map. Skipping the key.");
                continue;
            }

            try {
                Future<HBaseTaskResult>  taskFuture = taskTransactionBuffer.get(submittedTaskUuid).getTaskFuture();
                if (taskFuture == null) {
                    continue;
                }

                // Process done tasks
                if (taskFuture.isDone()) {
                    LOGGER.debug("Task " + submittedTaskUuid + " is done");

                    HBaseTaskResult taskResult = taskFuture.get(); // raise exceptions if any
                    boolean taskSucceeded = taskResult.isTaskSucceeded();

                    TaskStatus statusOfDoneTask = taskResult.getTaskStatus();

                    if (statusOfDoneTask == TaskStatus.WRITE_SUCCEEDED) {
                        if (!taskSucceeded) {
                            throw new Exception("Inconsistent success reports for task " + submittedTaskUuid);
                        }

                        // Do the accounting needed when task is successfully committed
                        LastCommittedPositionCheckpoint newCheckPoint =
                            notYetCommittedTasksAccountant.doAccountingOnTaskSuccess(
                                taskTransactionBuffer,
                                submittedTaskUuid
                            );

                        if (newCheckPoint != null) {
                            latestCommittedPseudoGTIDCheckPoint = newCheckPoint;
                        } else {
                            LOGGER.debug("No new checkpoint found.");
                        }

                        // metrics
                        applierTasksSucceededCounter.inc();

                    } else if (statusOfDoneTask == TaskStatus.WRITE_FAILED) {
                        if (taskSucceeded) {
                            throw new Exception("Inconsistent failure reports for task " + submittedTaskUuid);
                        }
                        LOGGER.warn("Task " + submittedTaskUuid + " failed. Task will be retried.");
                        requeueTask(submittedTaskUuid);
                        applierTasksFailedCounter.inc();
                    } else {
                        LOGGER.error("Illegal task status ["
                                + statusOfDoneTask
                                + "]. Probably a silent death of a thread. "
                                + "Will consider the task as failed and re-queue.");
                        requeueTask(submittedTaskUuid);
                        applierTasksFailedCounter.inc();
                    }
                }
            } catch (ExecutionException ex) {
                LOGGER.error(String.format("Future failed for task %s, with exception: %s",
                        submittedTaskUuid,
                        ex.getCause()));
                requeueTask(submittedTaskUuid);
                applierTasksFailedCounter.inc();
            } catch (NullPointerException e) {
                LOGGER.error("Null pointer", e);
            } catch (InterruptedException ei) {
                LOGGER.info(String.format("Task %s was canceled by interrupt. "
                        + "The task that has been canceled "
                        + "will be retired later by another future.", submittedTaskUuid), ei);
                requeueTask(submittedTaskUuid);
                applierTasksFailedCounter.inc();
            } catch (CancellationException ce) {
                LOGGER.error(String.format("Future failed for task %s, with exception: %s",
                        submittedTaskUuid ,
                        ce));
                requeueTask(submittedTaskUuid);
                applierTasksFailedCounter.inc();
            } catch (TaskAccountingException e) {
                LOGGER.error("FATAL: Task accounting exception", e);
                throw new ApplierException("Task accounting exception.");
            } catch (Exception e) {
                LOGGER.error(String.format("Exception for task %s. Will retry the task.", submittedTaskUuid),e);
                requeueTask(submittedTaskUuid);
                applierTasksFailedCounter.inc();
            }
        }
    }

    /**
     * Requeue task.
     *
     * @param failedTaskUuid UUID
     */
    private void requeueTask(String failedTaskUuid) {
        // keep the mutation buffer, just change the status so this task is picked up again
        taskTransactionBuffer.get(failedTaskUuid).setTaskFuture(null);
        taskTransactionBuffer.get(failedTaskUuid).setTaskStatus(TaskStatus.READY_FOR_PICK_UP);
    }

    private Integer taskRowsBuffered(String taskUuid) {

        int taskHasRows = 0;

        Map<String, TransactionProxy> task = taskTransactionBuffer.get(taskUuid);

        for (String transactionUuid : task.keySet()) {
            for (String tableName : task.get(transactionUuid).keySet()) {
                List<AugmentedRow> bufferedOPS = task.get(transactionUuid).get(tableName);
                if (bufferedOPS != null) {
                    taskHasRows += bufferedOPS.size();
                }
            }
        }

        return taskHasRows;
    }

    /**
     * Submit tasks that are READY_FOR_PICK_UP.
     *
     * @throws TaskBufferInconsistencyException Task is marked as READY_FOR_PICK_UP, but has no rows
     */
    public void submitTasksThatAreReadyForPickUp() throws IOException, TaskBufferInconsistencyException {

        if ((! DRY_RUN) && (hbaseConnection == null)) {
            LOGGER.info("HBase connection is gone. Will try to recreate new connection...");
            hbaseConnection = createHBaseConnection(10);
        }

        // one future per task
        for (final String taskUuid : taskTransactionBuffer.keySet()) {

            boolean taskHasRows = false;

            Map<String, TransactionProxy> task = taskTransactionBuffer.get(taskUuid);
            if (task == null) {
                throw new RuntimeException(String.format("Task %s is null", taskUuid));
            }

            for (String transactionUuid : task.keySet()) {
                Set<String> transactionTables = task.get(transactionUuid).keySet();
                for (String tableName : transactionTables) {
                    List<AugmentedRow> bufferedOPS = task.get(transactionUuid).get(tableName);
                    if (bufferedOPS != null && bufferedOPS.size() > 0) {
                        taskHasRows = true;
                    } else {
                        LOGGER.info("Table " + tableName + " has no rows!!!");
                    }
                }
            }

            // submit task
            if ((taskTransactionBuffer.get(taskUuid).getTaskStatus() == TaskStatus.READY_FOR_PICK_UP)) {
                if (taskHasRows) {
                    LOGGER.info("Submitting task " + taskUuid);

                    taskTransactionBuffer.get(taskUuid).setTaskStatus(TaskStatus.TASK_SUBMITTED);

                    // add to notYetCommittedList, unless its already there (hapens when failed task
                    // is requeued since task UUID is not changes on requeue - that way we know the
                    // order of tasks that corresponds to the binlog irregardless of possible task
                    // requeuing)
                    if (!notYetCommittedTasksAccountant.containsTaskUUID(taskUuid)) {
                        notYetCommittedTasksAccountant.addTaskUUID(taskUuid);
                    }

                    applierTasksSubmittedCounter.inc();

                    taskTransactionBuffer.get(taskUuid).setTaskFuture(
                        taskPool.submit(new HBaseWriterTask(
                                hbaseConnection,
                                mutationGenerator,
                                taskUuid,
                                taskTransactionBuffer.get(taskUuid),
                                validationService,
                                DRY_RUN
                        )
                    ));
                } else {
                    LOGGER.error("Task is marked as READY_FOR_PICK_UP, but has no rows");
                    throw new TaskBufferInconsistencyException("Task is marked as READY_FOR_PICK_UP, but has no rows.");
                }
            }
        }
    }
}
