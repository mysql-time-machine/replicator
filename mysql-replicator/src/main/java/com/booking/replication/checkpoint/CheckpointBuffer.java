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
        writableBuffer.add(checkpoint);
        bufferWriteLock.unlock();
        return;
    }

    public List<Checkpoint> getBufferedSoFar() {
        bufferWriteLock.lock();
        readableBuffer = writableBuffer;
        writableBuffer = new ArrayList<>();
        bufferWriteLock.unlock();
        return readableBuffer;
    }

}
