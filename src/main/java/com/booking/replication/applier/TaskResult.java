package com.booking.replication.applier;

import com.booking.replication.util.MutableLong;

import java.util.HashMap;

/**
 * Created by bosko on 3/17/16.
 */
public class TaskResult {

    private final String  taskUUID;
    private final boolean taskSucceeded;
    private final long    numberOfRowsInTask;
    private final HashMap<String, MutableLong> tableStats;

    public TaskResult(String uuid, boolean success, long numRowsInTask, HashMap<String, MutableLong> tableStats) {
        numberOfRowsInTask = numRowsInTask;
        taskSucceeded = success;
        taskUUID = uuid;
        this.tableStats = tableStats;
    }

    public String getTaskUUID() {
        return taskUUID;
    }

    public boolean isTaskSucceeded() {
        return taskSucceeded;
    }

    public long getNumberOfRowsInTask() {
        return numberOfRowsInTask;
    }

    public HashMap<String, MutableLong> getTableStats() {
        return tableStats;
    }
}
