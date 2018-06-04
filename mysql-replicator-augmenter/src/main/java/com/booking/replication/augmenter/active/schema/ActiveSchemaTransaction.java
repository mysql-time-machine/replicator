package com.booking.replication.augmenter.active.schema;

import com.booking.replication.augmenter.model.AugmentedEventData;
import com.booking.replication.augmenter.model.TransactionAugmentedEventData;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class ActiveSchemaTransaction {
    private final AtomicBoolean started;
    private final AtomicReference<Queue<AugmentedEventData>> queue;
    private final AtomicLong timestamp;
    private final int limit;

    public ActiveSchemaTransaction(int limit) {
        this.started = new AtomicBoolean();
        this.queue = new AtomicReference<>();
        this.timestamp = new AtomicLong();
        this.limit = limit;
    }

    public boolean begin() {
        if (!this.started.getAndSet(true)) {
            this.queue.set(new ConcurrentLinkedQueue<>());
            this.timestamp.set(0L);
            return true;
        } else {
            return false;
        }
    }

    public boolean add(AugmentedEventData data) {
        if (this.started.get() && this.queue.get().size() <= this.limit) {
            return this.queue.get().offer(data);
        } else {
            return false;
        }
    }

    public boolean drop() {
        if (this.started.getAndSet(false)) {
            this.queue.set(null);
            this.timestamp.set(0L);
            return true;
        } else {
            return false;
        }
    }

    public boolean commit(long timestamp) {
        if (this.started.getAndSet(false)) {
            this.timestamp.set(timestamp);
            return true;
        } else {
            return false;
        }
    }

    public boolean started() {
        return this.started.get();
    }

    public boolean committed() {
        return !this.started.get() && this.queue.get() != null;
    }

    public long getTimestamp() {
        return this.timestamp.get();
    }

    public TransactionAugmentedEventData getData() {
        if (!this.started.get() && this.queue.get() != null) {
            return new TransactionAugmentedEventData(this.queue.getAndSet(null).toArray(new AugmentedEventData[0]));
        } else {
            return null;
        }
    }
}
