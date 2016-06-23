package com.booking.replication.applier.hbase;

import java.util.HashMap;
import java.util.concurrent.Future;

class ApplierTask extends HashMap<String, TransactionProxy> {
    private Future<HBaseTaskResult> taskFuture;
    private Integer taskStatus;

    ApplierTask(int taskStatus) {
        this(taskStatus, null);
    }

    ApplierTask(int taskStatus, Future<HBaseTaskResult> taskResultFuture) {
        super();
        setTaskStatus(taskStatus);
        setTaskFuture(taskResultFuture);
    }

    Integer getTaskStatus() {
        return taskStatus;
    }

    void setTaskStatus(Integer taskStatus) {
        this.taskStatus = taskStatus;
    }

    Future<HBaseTaskResult> getTaskFuture() {
        return taskFuture;
    }

    void setTaskFuture(Future<HBaseTaskResult> taskFuture) {
        this.taskFuture = taskFuture;
    }
}
