package com.booking.replication.applier.hbase;

import com.booking.replication.applier.TaskStatus;
import com.booking.replication.checkpoints.LastCommittedPositionCheckpoint;

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
     * Non-Committed task UUUDs in the order as they are received from the binlog.
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

    public synchronized LastCommittedPositionCheckpoint doAccountingOnTaskSuccess(
            ConcurrentHashMap<String, ApplierTask> taskTransactionBuffer,
            String committedTaskID) throws Exception {
        LOGGER.info("accounting on success of " + committedTaskID);
        LastCommittedPositionCheckpoint committedHeadPseudoGTIDCheckPoint = null;
        if (allLowerPositionTasksHaveBeenCommitted(taskTransactionBuffer, committedTaskID)) {
            int taskIndex = findTaskIndexInNotYetCommittedList(taskTransactionBuffer, committedTaskID);

            List<String> committedHead = taskHead(taskIndex);

            committedHeadPseudoGTIDCheckPoint =
                    scanCommittedTasksForPseudoGTIDCheckpoint(taskTransactionBuffer, committedHead);

            // remove taskHead
            List<String> committedTail = taskTail(taskIndex);
            notYetCommittedTaskUUIDs = committedTail;

            if (committedHeadPseudoGTIDCheckPoint != null) {
                System.out.println("New check point found in committed tasks" + committedHeadPseudoGTIDCheckPoint.toJson());
            }
        }
        return committedHeadPseudoGTIDCheckPoint;
    }

    private boolean allLowerPositionTasksHaveBeenCommitted(
            ConcurrentHashMap<String, ApplierTask> taskTransactionBuffer,
            String committedTaskUUID) {
        boolean result = false;
        LOGGER.info("Checking taskHead. Total items in notYetCommittedTaskUUIDs " + notYetCommittedTaskUUIDs.size());
        for (String taskUUID : notYetCommittedTaskUUIDs) {
            LOGGER.info(committedTaskUUID + " [is before or equal] " + taskUUID);
            if (taskUUID.equals(committedTaskUUID)) {
                LOGGER.info("this task is the first in the list or all previous have been committed");
                if (taskTransactionBuffer.get(taskUUID).getTaskStatus() != TaskStatus.WRITE_SUCCEEDED) {
                    LOGGER.error("Incosystecy!!!");
                    break;
                } else {
                    result = true;
                    break;
                }
            } else {
                if (taskTransactionBuffer.get(taskUUID).getTaskStatus() != TaskStatus.WRITE_SUCCEEDED) {
                    LOGGER.info("task before "
                            + taskUUID
                            + " has non success status => " + taskTransactionBuffer.get(taskUUID).getTaskStatus());
                    break;
                } else {
                    LOGGER.info("task before " + taskUUID + " has status => " + taskTransactionBuffer.get(taskUUID).getTaskStatus());
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

                break;

            } else {

                taskIndexInNotYetCommittedList++;

                System.out.println("Committed task "
                        + taskUUID
                        + "with index "
                        + taskIndexInNotYetCommittedList
                        + " will be removed from the notYetCommittedTaskUUIDs list");
            }
        }
        return taskIndexInNotYetCommittedList;
    }


    public List<String> taskHead(int taskIndex) {
        System.out.println("Filtering from 0 to " + taskIndex);
        return  notYetCommittedTaskUUIDs.subList(0, taskIndex + 1);
    }

    public List taskTail(int taskIndex) {
        return notYetCommittedTaskUUIDs.subList(taskIndex + 1, notYetCommittedTaskUUIDs.size());
    }

    private LastCommittedPositionCheckpoint scanCommittedTasksForPseudoGTIDCheckpoint(
            ConcurrentHashMap<String, ApplierTask> taskTransactionBuffer,
            List<String> committedHead) throws Exception {

        LastCommittedPositionCheckpoint latestApplierCommittedPseudoGTIDCheckPoint = null;

        for (String taskUUID : committedHead) {

            if (taskUUID.equals(committedHead.get(committedHead.size() - 1))) {

                // reached the end
                if (taskTransactionBuffer.get(taskUUID).getPseudoGTIDCheckPoint() != null) {

                    latestApplierCommittedPseudoGTIDCheckPoint = taskTransactionBuffer.get(taskUUID).getPseudoGTIDCheckPoint();

                    break;

                } else {

                    // no pGTID in this task

                }
            } else {
                if (taskTransactionBuffer.get(taskUUID) != null) {
                    if (taskTransactionBuffer.get(taskUUID).getTaskStatus() != TaskStatus.WRITE_SUCCEEDED) {

                        // the reason for throwing exception here is that this method should be called only if
                        // all previous tasks have been confirmed as committed
                        LOGGER.error(taskUUID + " -----> " + taskTransactionBuffer.get(taskUUID).getTaskStatus());
                        throw new TaskAccountingException("committedHead contains a task which is not WRITE_SUCCEEDED:"
                                + taskUUID);
                    } else {
                        if (taskTransactionBuffer.get(taskUUID).getPseudoGTIDCheckPoint() != null) {
                            latestApplierCommittedPseudoGTIDCheckPoint = taskTransactionBuffer.get(taskUUID).getPseudoGTIDCheckPoint();
                        }
                    }
                } else {
                    LOGGER.error("task "
                            + taskUUID
                            + " missing from taskTransactionBuffer, but it exists in notYetCommittedTaskUUIDs.");
                    throw new TaskAccountingException("task "
                        + taskUUID
                        + " missing from taskTransactionBuffer, but it exists in notYetCommittedTaskUUIDs.");
                }
            }
        }
        return latestApplierCommittedPseudoGTIDCheckPoint;
    }
}
