package com.booking.replication.coordinator;

import com.booking.replication.commons.checkpoint.CheckpointStorage;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;

public abstract class Coordinator implements LeaderCoordinator, CheckpointStorage {
    private static final Logger LOG = Logger.getLogger(Coordinator.class.getName());

    enum Type {
        ZOOKEEPER {
            @Override
            public Coordinator newInstance(Map<String, String> configuration) {
                return new ZookeeperCoordinator(configuration);
            }
        },
        FILE {
            @Override
            public Coordinator newInstance(Map<String, String> configuration) {
                return new FileCoordinator(configuration);
            }
        };

        public abstract Coordinator newInstance(Map<String, String> configuration);
    }

    interface Configuration {
        String TYPE = "coordinator.type";
    }
    private final AtomicReference<Runnable> takeRunnable;
    private final AtomicReference<Runnable> lossRunnable;
    private final AtomicBoolean hasLeadership;

    protected Coordinator() {
        this.takeRunnable = new AtomicReference<>(() -> {});
        this.lossRunnable = new AtomicReference<>(() -> {});
        this.hasLeadership = new AtomicBoolean();
    }

    @Override
    public void onLeadershipTake(Runnable runnable) {
        Objects.requireNonNull(runnable);

        this.takeRunnable.set(runnable);
    }

    @Override
    public void onLeadershipLoss(Runnable runnable) {
        Objects.requireNonNull(runnable);

        this.lossRunnable.set(runnable);
    }

    protected void takeLeadership() {
        try {
            if (!this.hasLeadership.getAndSet(true)) {
                this.takeRunnable.get().run();
            }
        } catch (Exception exception) {
            Coordinator.LOG.log(Level.SEVERE, "error taking leadership", exception);
        } finally {
            this.lossLeadership();
        }
    }

    protected void lossLeadership() {
        if (this.hasLeadership.getAndSet(false)) {
            this.lossRunnable.get().run();
        }
    }


    public abstract void start() throws InterruptedException;

    public abstract void wait(long timeout, TimeUnit unit) throws InterruptedException;

    public abstract void join() throws InterruptedException;

    public abstract void stop() throws InterruptedException;

    public static Coordinator build(Map<String, String> configuration) {
        return Type.valueOf(
                configuration.getOrDefault(Configuration.TYPE, Type.FILE.name())
        ).newInstance(configuration);
    }
}
