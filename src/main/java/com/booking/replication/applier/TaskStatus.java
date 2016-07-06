package com.booking.replication.applier;

/**
 * Created by bdevetak on 07/12/15.
 */
public enum TaskStatus {
    READY_FOR_BUFFERING,
    READY_FOR_PICK_UP,
    TASK_SUBMITTED,
    WRITE_IN_PROGRESS,
    WRITE_FAILED,
    WRITE_SUCCEEDED
}
