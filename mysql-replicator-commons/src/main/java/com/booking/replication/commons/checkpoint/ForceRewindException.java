package com.booking.replication.commons.checkpoint;

public class ForceRewindException extends Exception {
    private Checkpoint checkpoint;

    public ForceRewindException(String message, Throwable cause, Checkpoint checkpoint) {
        super(message, cause);
        this.checkpoint = checkpoint;
    }

    public ForceRewindException(String message, Checkpoint checkpoint) {
        this(message, null, checkpoint);
    }

    public Checkpoint getCheckpoint() {
        return this.checkpoint;
    }
}
