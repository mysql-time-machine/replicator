package com.booking.replication.applier.hbase;

/**
 * Created by bosko on 3/17/16.
 */
public class HBaseTaskResult {

    private final String taskUuid;
    private final int taskStatus;
    private final boolean taskSucceeded;

    /**
     * Result after running HBase task.
     * @param uuid      Task UUID
     * @param status    Task Status
     * @param success   Task success
     */
    public HBaseTaskResult(
            String uuid,
            int status,
            boolean success
    ) {
        taskSucceeded = success;
        taskUuid = uuid;
        taskStatus = status;
    }

    public String getTaskUuid() {
        return taskUuid;
    }

    public boolean isTaskSucceeded() {
        return taskSucceeded;
    }

    public int getTaskStatus() {
        return taskStatus;
    }

}
