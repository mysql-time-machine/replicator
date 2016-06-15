package com.booking.replication.applier;

/**
 * Created by bosko on 3/17/16.
 */
public class TaskResult {

    private final String                                        taskUUID;
    private final int                                           taskStatus;
    private final boolean                                       taskSucceeded;

    public TaskResult(
            String uuid,
            int status,
            boolean success
    ) {
        taskSucceeded                        = success;
        taskUUID                             = uuid;
        taskStatus                           = status;
    }

    public String getTaskUUID() {
        return taskUUID;
    }

    public boolean isTaskSucceeded() {
        return taskSucceeded;
    }

    public int getTaskStatus() {
        return taskStatus;
    }

}
