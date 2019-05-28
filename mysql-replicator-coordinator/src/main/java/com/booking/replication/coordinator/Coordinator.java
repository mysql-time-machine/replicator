package com.booking.replication.coordinator;

import com.booking.replication.commons.checkpoint.CheckpointStorage;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public abstract class Coordinator implements LeaderCoordinator, CheckpointStorage {
    private static final Logger LOG = LogManager.getLogger(Coordinator.class);

    public enum Type {
        ZOOKEEPER {
            @Override
            public Coordinator newInstance(Map<String, Object> configuration) {
                return new ZookeeperCoordinator(configuration);
            }
        },
        FILE {
            @Override
            public Coordinator newInstance(Map<String, Object> configuration) {
                return new FileCoordinator(configuration);
            }
        };

        public abstract Coordinator newInstance(Map<String, Object> configuration);
    }

    public interface Configuration {
        String TYPE = "coordinator.type";
    }

    private static final long WAIT_STEP_MILLIS = 100;

    private final ExecutorService executor;
    private final AtomicBoolean running;
    private final Semaphore semaphore;
    private final AtomicBoolean hasLeadership;
    private final AtomicBoolean lostLeadership;
    private final AtomicReference<Runnable> takeRunnable;
    private final AtomicReference<Runnable> loseRunnable;

    protected Coordinator() {
        this.executor = Executors.newSingleThreadExecutor();
        this.running = new AtomicBoolean(false);
        this.semaphore = new Semaphore(0);
        this.hasLeadership = new AtomicBoolean(false);
        this.lostLeadership = new AtomicBoolean(true);
        this.takeRunnable = new AtomicReference<>(() -> {});
        this.loseRunnable = new AtomicReference<>(() -> {});
    }

    @Override
    public void onLeadershipTake(Runnable runnable) {
        Objects.requireNonNull(runnable);
        this.takeRunnable.set(runnable);
    }

    @Override
    public void onLeadershipLose(Runnable runnable) {
        Objects.requireNonNull(runnable);
        this.loseRunnable.set(runnable);
    }

    protected void takeLeadership() {
        if (!this.hasLeadership.getAndSet(true)) {
            try {
                    this.awaitLeadership();
                    this.lostLeadership.set(false);

                    try {
                        this.takeRunnable.get().run();
                    } catch (Exception exception) {
                        Coordinator.LOG.error("error taking leadership", exception);
                    }

                    this.semaphore.acquire();
            } catch (Exception exception) {
                Coordinator.LOG.warn("cannot take leadership");
            } finally {
                if (!this.lostLeadership.getAndSet(true)) {
                    this.hasLeadership.set(false);
                    this.loseRunnable.get().run();
                }
            }
        }
    }

    protected void loseLeadership() {
        if (!this.lostLeadership.getAndSet(true)) {
            this.hasLeadership.set(false);
            this.loseRunnable.get().run();
            this.semaphore.release();
        }
    }

    public void start() {
        if (!this.running.getAndSet(true)) {
            this.executor.execute(this::takeLeadership);
        }
    }

    public abstract void awaitLeadership();

    public void stop() {
        if (this.running.getAndSet(false)) {
            try {
                this.loseLeadership();
                this.executor.shutdown();
                this.executor.awaitTermination(5L, TimeUnit.SECONDS);
            } catch (InterruptedException exception) {
                Coordinator.LOG.error("error stopping coordinator", exception);
            } finally {
                this.executor.shutdownNow();
            }
        }
    }

    public void wait(long timeout, TimeUnit unit) {
        try {
            long remainMillis = unit.toMillis(timeout);

            while (remainMillis > 0) {
                long sleepMillis = remainMillis > Coordinator.WAIT_STEP_MILLIS ? Coordinator.WAIT_STEP_MILLIS : remainMillis;
                Thread.sleep(sleepMillis);
                remainMillis -= sleepMillis;
            }
        } catch (InterruptedException exception) {
            Coordinator.LOG.error("error waiting coordinator", exception);
        }
    }

    public void join() {
        this.wait(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
    }

    public static Coordinator build(Map<String, Object> configuration) {
        return Type.valueOf(
                configuration.getOrDefault(Configuration.TYPE, Type.FILE.name()).toString()
        ).newInstance(configuration);
    }
}
