package com.booking.replication.commons.checkpoint;

public class ForceRewindException extends RuntimeException {
    public ForceRewindException(String message) {
        super(message);
    }
}
