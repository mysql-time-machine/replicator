package com.booking.replication.applier.hbase;

import com.booking.replication.applier.TaskStatus;
import com.booking.replication.checkpoints.PseudoGTIDCheckpoint;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by bosko on 9/15/16.
 */
public class HBaseApplierNotYetCommittedAccounting {

    private static final Logger LOGGER = LoggerFactory.getLogger(HBaseApplierNotYetCommittedAccounting.class);

    /**
     * Non-Committed task UUIDs in the order as they are received from the binlog.
     * Since tasks run in parallel, and the binlog is ordered structure, we have
     * a problem of knowing when a specific position in the binlog has been succesfully
     * committed into hbase since we need to know that all tasks that correspond to
     * earlier positions have also been committed. If that is true, than we can mark
     * a safe checkpoint.
     */
    private List<String> notYetCommittedTaskUUIDs = new ArrayList<>();

    public synchronized void addTaskUUID(String submittedTaskUUID) {
        notYetCommittedTaskUUIDs.add(submittedTaskUUID);
    }

    public synchronized boolean containsTaskUUID(String taskUUID) {
        for (String notYetCommittedTaskUUID : notYetCommittedTaskUUIDs) {
            if (notYetCommittedTaskUUID.equals(taskUUID)) {
                return true;
            }
        }
        return false;
    }

    public synchronized PseudoGTIDCheckpoint doAccountingOnTaskSuccess(
            ConcurrentHashMap<String, ApplierTask> taskTransactionBuffer,
            String committedTaskID) throws Exception {

        LOGGER.debug("Accounting on success of " + committedTaskID);

        // update status in the taskTransactionBuffer
        taskTransactionBuffer.get(committedTaskID).setTaskStatus(TaskStatus.WRITE_SUCCEEDED);

        PseudoGTIDCheckpoint committedHeadPseudoGTIDCheckPoint = null;

        if (allLowerPositionTasksHaveBeenCommitted(taskTransactionBuffer, committedTaskID)) {

            int taskIndex = findTaskIndexInNotYetCommittedList(taskTransactionBuffer, committedTaskID);

            List<String> committedHead = taskHead(taskIndex);

            committedHeadPseudoGTIDCheckPoint =
                    scanCommittedTasksForPseudoGTIDCheckpoint(taskTransactionBuffer, committedHead);

            // remove committed tasks from main buffer taskTransactionBuffer
            //   note: the buffer is structured by task-transaction, so
            //         if there is an open transaction UUID in this task, it has
            //         already been copied to the new/next task
            for (String taskToBeRemovedUUID : committedHead) {
                LOGGER.debug("Removing committedHead from taskTransactionBuffer");
                taskTransactionBuffer.remove(taskToBeRemovedUUID);
                LOGGER.debug("Removed task " + taskToBeRemovedUUID + " from taskTransactionBuffer.");
            }

            // remove taskHead
            List<String> committedTail = taskTail(taskIndex);
            notYetCommittedTaskUUIDs = committedTail;

            if (committedHeadPseudoGTIDCheckPoint != null) {
                LOGGER.debug("New check point found in committed tasks" + committedHeadPseudoGTIDCheckPoint.toJson());
            }
        }
        return committedHeadPseudoGTIDCheckPoint;
    }

    private boolean allLowerPositionTasksHaveBeenCommitted(
            ConcurrentHashMap<String, ApplierTask> taskTransactionBuffer,
            String committedTaskUUID) {
        boolean result = false;
        LOGGER.debug("Checking taskHead. Total items in notYetCommittedTaskUUIDs " + notYetCommittedTaskUUIDs.size());
        for (String taskUUID : notYetCommittedTaskUUIDs) {
            LOGGER.debug(committedTaskUUID + " [is before or equal] " + taskUUID);
            if (taskUUID.equals(committedTaskUUID)) {
                if (taskTransactionBuffer.get(taskUUID).getTaskStatus() != TaskStatus.WRITE_SUCCEEDED) {
                    LOGGER.error("Unexpected task status for task " + taskUUID);
                    break;
                } else {
                    result = true;
                    break;
                }
            } else {
                if (taskTransactionBuffer.get(taskUUID).getTaskStatus() != TaskStatus.WRITE_SUCCEEDED) {
                    break;
                }
            }
        }
        return result;
    }

    private int findTaskIndexInNotYetCommittedList(
            ConcurrentHashMap<String, ApplierTask> taskTransactionBuffer,
            String committedTaskUUID) {
        int taskIndexInNotYetCommittedList = 0;
        for (String taskUUID : notYetCommittedTaskUUIDs) {
            if (taskUUID.equals(committedTaskUUID)) {
                LOGGER.debug("Committed task "
                        + taskUUID
                        + "with index "
                        + taskIndexInNotYetCommittedList
                        + " will be removed from the notYetCommittedTaskUUIDs list");
                break;
            } else {
                taskIndexInNotYetCommittedList++;
                LOGGER.debug("Committed task "
                        + taskUUID
                        + "with index "
                        + taskIndexInNotYetCommittedList
                        + " will be removed from the notYetCommittedTaskUUIDs list");
            }
        }
        LOGGER.debug("taskIndex in notCommittedList: { " +  committedTaskUUID + " => " + taskIndexInNotYetCommittedList);
        return taskIndexInNotYetCommittedList;
    }


    private List<String> taskHead(int taskIndex) {
        return new ArrayList<>(notYetCommittedTaskUUIDs.subList(0, taskIndex + 1));
    }

    private List taskTail(int taskIndex) {
        return new ArrayList<>(notYetCommittedTaskUUIDs.subList(taskIndex + 1, notYetCommittedTaskUUIDs.size()));
    }

    private PseudoGTIDCheckpoint scanCommittedTasksForPseudoGTIDCheckpoint(
            ConcurrentHashMap<String, ApplierTask> taskTransactionBuffer,
            List<String> committedHead) throws Exception {

        LOGGER.debug("scanCommittedTasksForPseudoGTIDCheckpoint. Loop committedHead list");

        PseudoGTIDCheckpoint latestApplierCommittedPseudoGTIDCheckPoint = null;

        for (String taskUUID : committedHead) {

            LOGGER.debug("scanning taskUUID " + taskUUID);

            if (taskUUID.equals(committedHead.get(committedHead.size() - 1))) {

                // reached the end
                LOGGER.debug("reached the end of committedHead, index: " + (committedHead.size() - 1));

                if (taskTransactionBuffer.get(taskUUID).getPseudoGTIDCheckPoint() != null) {

                    LOGGER.debug("last task " + taskUUID + " commited head contains pGTID" + taskTransactionBuffer.get(taskUUID).getPseudoGTIDCheckPoint().getPseudoGTID());

                    latestApplierCommittedPseudoGTIDCheckPoint = taskTransactionBuffer.get(taskUUID).getPseudoGTIDCheckPoint();

                    LOGGER.debug("set latestApplierCommittedPseudoGTIDCheckPoint to " + taskTransactionBuffer.get(taskUUID).getPseudoGTIDCheckPoint().getPseudoGTID());
                    break;

                } else {
                    // no pGTID in this task
                    LOGGER.debug("No pGTID in task " + taskUUID); // check
                }
            } else {

                if (taskTransactionBuffer.get(taskUUID) != null) {

                    LOGGER.debug("not at last task in committed head");

                    if (taskTransactionBuffer.get(taskUUID).getTaskStatus() != TaskStatus.WRITE_SUCCEEDED) {

                        // the reason for throwing exception here is that this method should be called only if
                        // all previous tasks have been confirmed as committed
                        throw new TaskAccountingException("committedHead contains a task which is not WRITE_SUCCEEDED:"
                                + taskUUID);
                    } else {
                        if (taskTransactionBuffer.get(taskUUID).getPseudoGTIDCheckPoint() != null) {

                            latestApplierCommittedPseudoGTIDCheckPoint = taskTransactionBuffer.get(taskUUID).getPseudoGTIDCheckPoint();
                            LOGGER.debug("set latestApplierCommittedPseudoGTIDCheckPoint to "
                                         + taskTransactionBuffer.get(taskUUID).getPseudoGTIDCheckPoint().getPseudoGTID());
                        }
                    }
                } else {
                    throw new TaskAccountingException("Task "
                        + taskUUID
                        + " missing from taskTransactionBuffer, but it exists in notYetCommittedTaskUUIDs.");
                }
            }
        }
        // This should be the laster pGTID observed by the applier in the commitedHead list, which is a list
        // tasks, in the order that corresponds to the binlog, which has been successfully committed
        return latestApplierCommittedPseudoGTIDCheckPoint;
    }
}
