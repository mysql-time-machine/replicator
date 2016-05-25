package com.booking.replication.applier;

import com.booking.replication.util.MutableLong;

import java.util.HashMap;

/**
 * Created by bosko on 3/17/16.
 */
public class TaskResult {

    private final String                                        taskUUID;
    private final boolean                                       taskSucceeded;
    private final long                                          numberOfAugmentedRowsInTask;
    private final long                                          numberOfAffectedHBaseRowsInTask;
    private final HashMap<String, HashMap<String, MutableLong>> tableStats;

    public TaskResult(String uuid,
                      boolean success,
                      long numOfAugmentedRowsInTask,
                      long numberOfAffectedHBaseRowsInTask,
                      HashMap<String, HashMap<String, MutableLong>> tableStats) {
        numberOfAugmentedRowsInTask          = numOfAugmentedRowsInTask;
        taskSucceeded                        = success;
        taskUUID                             = uuid;
        this.tableStats                      = tableStats;
        this.numberOfAffectedHBaseRowsInTask = numberOfAffectedHBaseRowsInTask;
    }

    public String getTaskUUID() {
        return taskUUID;
    }

    public boolean isTaskSucceeded() {
        return taskSucceeded;
    }

    public long getNumberOfAugmentedRowsInTask() {
        return numberOfAugmentedRowsInTask;
    }

    public HashMap<String, HashMap<String, MutableLong>> getTableStats() {
        return tableStats;
    }

    public long getNumberOfAffectedHBaseRowsInTask() {
        return numberOfAffectedHBaseRowsInTask;
    }
}
