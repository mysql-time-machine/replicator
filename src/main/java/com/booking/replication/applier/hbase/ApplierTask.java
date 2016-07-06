package com.booking.replication.applier.hbase;

import com.booking.replication.applier.TaskStatus;

import java.util.HashMap;
import java.util.concurrent.Future;

class ApplierTask extends HashMap<String, TransactionProxy> {
    private Future<HBaseTaskResult> taskFuture;
    private TaskStatus taskStatus;

    ApplierTask(TaskStatus taskStatus) {
        this(taskStatus, null);
    }

    ApplierTask(TaskStatus taskStatus, Future<HBaseTaskResult> taskResultFuture) {
        super();
        setTaskStatus(taskStatus);
        setTaskFuture(taskResultFuture);
    }

    TaskStatus getTaskStatus() {
        return taskStatus;
    }

    void setTaskStatus(TaskStatus taskStatus) {
        this.taskStatus = taskStatus;
    }

    Future<HBaseTaskResult> getTaskFuture() {
        return taskFuture;
    }

    void setTaskFuture(Future<HBaseTaskResult> taskFuture) {
        this.taskFuture = taskFuture;
    }
}
