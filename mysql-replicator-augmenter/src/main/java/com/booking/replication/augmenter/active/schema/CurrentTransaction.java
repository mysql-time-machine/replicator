package com.booking.replication.augmenter.active.schema;

import com.booking.replication.augmenter.model.AugmentedEvent;
import com.booking.replication.augmenter.model.TransactionAugmentedEventData;

import java.util.ArrayList;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class CurrentTransaction {
    private static final long DEFAULT_XXID = 0L;

    private final AtomicBoolean started;
    private final AtomicBoolean resuming;
    private final AtomicReference<UUID> identifier;
    private final AtomicReference<Queue<AugmentedEvent>> eventQueue;
    private final AtomicLong xxid;
    private final AtomicLong timestamp;
    private final int sizeLimit;

    public CurrentTransaction(int sizeLimit) {
        this.started = new AtomicBoolean();
        this.resuming = new AtomicBoolean();
        this.identifier = new AtomicReference<>();
        this.eventQueue = new AtomicReference<>();
        this.xxid = new AtomicLong();
        this.timestamp = new AtomicLong();
        this.sizeLimit = sizeLimit;
    }

    public boolean begin() {
        if (!this.started.getAndSet(true)) {
            this.resuming.set(false);
            this.identifier.set(UUID.randomUUID());
            this.eventQueue.set(new ConcurrentLinkedQueue<>());
            this.xxid.set(0L);
            this.timestamp.set(0L);
            return true;
        } else {
            this.resuming.set(true);
            this.eventQueue.set(new ConcurrentLinkedQueue<>());
            return false;
        }
    }

    public boolean add(AugmentedEvent event) {
        if (this.started.get() && !this.sizeLimitExceeded()) {
            return this.eventQueue.get().offer(event);
        } else {
            return false;
        }
    }

    public TransactionAugmentedEventData clean() {
        if (this.eventQueue.get() != null) {
            return new TransactionAugmentedEventData(
                    this.identifier.get(),
                    this.xxid.get(),
                    new ArrayList<>(this.eventQueue.getAndSet((this.resuming.get())?(new ConcurrentLinkedQueue<>()):(null)))
            );
        } else {
            return null;
        }
    }

    public boolean commit(long xxid, long timestamp) {
        if (this.started.getAndSet(false)) {
            this.xxid.set(xxid);
            this.timestamp.set(timestamp);
            return true;
        } else {
            return false;
        }
    }

    public boolean commit(long timestamp) {
        return this.commit(CurrentTransaction.DEFAULT_XXID, timestamp);
    }

    public boolean started() {
        return this.started.get();
    }

    public boolean resuming() {
        return this.resuming.get();
    }

    public boolean committed() {
        return !this.started.get() && this.eventQueue.get() != null;
    }

    public boolean sizeLimitExceeded() {
        return this.eventQueue.get().size() >= this.sizeLimit;
    }

    public long getTimestamp() {
        return this.timestamp.get();
    }
}
