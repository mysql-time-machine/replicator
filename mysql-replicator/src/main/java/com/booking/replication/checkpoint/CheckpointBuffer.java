package com.booking.replication.checkpoint;

import com.booking.replication.commons.checkpoint.Checkpoint;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class CheckpointBuffer {

    private List<Checkpoint> writableBuffer;
    private List<Checkpoint> readableBuffer;
    private Lock bufferWriteLock;

    public CheckpointBuffer() {
        bufferWriteLock = new ReentrantLock();
        this.writableBuffer = new ArrayList<>();
    }

    public void writeToBuffer(Checkpoint checkpoint) {
        bufferWriteLock.lock();
        try {
            writableBuffer.add(checkpoint);
        } finally {
            bufferWriteLock.unlock();
        }
    }

    public List<Checkpoint> getBufferedSoFar() {
        bufferWriteLock.lock();
        try {
            readableBuffer = writableBuffer;
            writableBuffer = new ArrayList<>();
        } finally {
            bufferWriteLock.unlock();
        }
        return readableBuffer;
    }

}
