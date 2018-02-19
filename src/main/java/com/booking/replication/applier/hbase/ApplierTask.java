package com.booking.replication.applier.hbase;

import com.booking.replication.applier.TaskStatus;
import com.booking.replication.checkpoints.PseudoGTIDCheckpoint;

import java.util.HashMap;
import java.util.concurrent.Future;

class ApplierTask extends HashMap<String, TransactionProxy> {
    private Future<HBaseTaskResult> taskFuture;
    private TaskStatus taskStatus;

    // TODO: rename PseudoGTIDCheckpoint since its no longer just
    //       for committed positions
    //       One task can contain row ops sequence that spance across more than
    //       one pGTID, so the last seen is maintained in the task
    private PseudoGTIDCheckpoint pseudoGTIDCheckPoint; // <- latest one withing the task event range

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

    public PseudoGTIDCheckpoint getPseudoGTIDCheckPoint() {
        return pseudoGTIDCheckPoint;
    }

    public void setPseudoGTIDCheckPoint(PseudoGTIDCheckpoint pseudoGTIDCheckPoint) {
        this.pseudoGTIDCheckPoint = pseudoGTIDCheckPoint;
    }
}
