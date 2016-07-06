package com.booking.replication.applier.hbase;

import com.booking.replication.applier.TaskStatus;

/**
 * Created by bosko on 3/17/16.
 */
public class HBaseTaskResult {

    private final String taskUuid;
    private final TaskStatus taskStatus;
    private final boolean taskSucceeded;

    /**
     * Result after running HBase task.
     * @param uuid      Task UUID
     * @param status    Task Status
     * @param success   Task success
     */
    public HBaseTaskResult(
            String uuid,
            TaskStatus status,
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

    public TaskStatus getTaskStatus() {
        return taskStatus;
    }

}
