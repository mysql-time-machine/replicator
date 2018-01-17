package com.booking.replication.coordinator;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class FileCoordinator implements Coordinator {
    private final ExecutorService executor;
    private final String path;
    private final List<Runnable> takeRunnableList;
    private final List<Runnable> lossRunnableList;
    private final AtomicBoolean running;
    private final AtomicBoolean hasLeadership;

    FileCoordinator(Map<String, String> configuration) {
        this.executor = Executors.newSingleThreadExecutor();
        this.path = configuration.getOrDefault(Configuration.LEADERSHIP_PATH, "/tmp/leadership.coordinator");
        this.takeRunnableList = new ArrayList<>();
        this.lossRunnableList = new ArrayList<>();
        this.running = new AtomicBoolean();
        this.hasLeadership = new AtomicBoolean();
    }

    @Override
    public void storeCheckpoint(String path, byte[] checkpoint) throws IOException {
        Files.write(Paths.get(path), checkpoint);
    }

    @Override
    public byte[] loadCheckpoint(String path) throws IOException {
        try {
            return Files.readAllBytes(Paths.get(path));
        } catch (NoSuchFileException exception) {
            return null;
        }
    }

    @Override
    public void onLeadershipTake(Runnable runnable) {
        this.takeRunnableList.add(runnable);
    }

    @Override
    public void onLeadershipLoss(Runnable runnable) {
        this.lossRunnableList.add(runnable);
    }

    @Override
    public String defaultCheckpointPath() {
        return "/tmp/checkpoint.coordinator";
    }

    @Override
    public void start() {
        if (!this.running.getAndSet(true)) {
            this.executor.execute(() -> {
                while (this.running.get()) {
                    try (FileChannel fileChannel = FileChannel.open(Paths.get(this.path), StandardOpenOption.WRITE, StandardOpenOption.CREATE);
                         FileLock fileLock = fileChannel.tryLock()) {
                        if (fileLock.isValid()) {
                            try {
                                if (!this.hasLeadership.getAndSet(true)) {
                                    this.takeRunnableList.forEach(Runnable::run);
                                }
                            } finally {
                                if (this.hasLeadership.getAndSet(false)) {
                                    this.lossRunnableList.forEach(Runnable::run);
                                }
                            }
                        }
                    } catch (Exception exception) {
                        try {
                            Thread.sleep(100L);
                        } catch (InterruptedException ignored) {
                        }
                    }
                }
            });
        }
    }

    @Override
    public final void wait(long timeout, TimeUnit unit) throws InterruptedException {
        if (this.running.get()) {
            this.executor.awaitTermination(timeout, unit);
        }
    }

    @Override
    public final void join() throws InterruptedException {
        this.wait(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
    }

    @Override
    public final void stop() throws InterruptedException {
        if (this.running.getAndSet(false)) {
            try {
                if (this.hasLeadership.getAndSet(false)) {
                    this.lossRunnableList.forEach(Runnable::run);
                }

                this.executor.shutdown();
                this.executor.awaitTermination(5L, TimeUnit.SECONDS);
            } finally {
                this.executor.shutdownNow();
            }
        }
    }
}
