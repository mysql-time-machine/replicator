package com.booking.replication.exceptions;

public class ReplicatorStartException extends Exception {

    public ReplicatorStartException() {
        super("");
    }

    public ReplicatorStartException(String message) {
        super(message);
    }

    public ReplicatorStartException(Exception e, String message) {
        super(message, e);
    }
}
