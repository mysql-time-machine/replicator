package com.booking.replication.exceptions;

public class CheckpointException extends Exception {

    public CheckpointException() {
        super("");
    }

    public CheckpointException(String message) {
        super(message);
    }

    public CheckpointException(Exception e, String message) {
        super(message, e);
    }
}
