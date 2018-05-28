package com.booking.replication.coordinator;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class FileCoordinator extends Coordinator {
    public interface Configuration {
        String LEADERSHIP_PATH = "file.leadership.path";
    }

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final ExecutorService executor;
    private final AtomicBoolean running;
    private final String path;

    public FileCoordinator(Map<String, String> configuration) {
        this.executor = Executors.newSingleThreadExecutor();
        this.running = new AtomicBoolean();
        this.path = configuration.getOrDefault(Configuration.LEADERSHIP_PATH, "/tmp/leadership.coordinator");
    }

    @Override
    public <Type> void storeCheckpoint(String path, Type checkpoint) throws IOException {
        if (checkpoint != null) {
            Files.write(Paths.get(path), FileCoordinator.MAPPER.writeValueAsBytes(checkpoint));
        }
    }

    @Override
    public <Type> Type loadCheckpoint(String path, Class<Type> type) throws IOException {
        try {
            byte[] bytes = Files.readAllBytes(Paths.get(path));

            if (bytes.length > 0) {
                return FileCoordinator.MAPPER.readValue(bytes, type);
            } else {
                return null;
            }
        } catch (NoSuchFileException exception) {
            return null;
        }
    }

    @Override
    public void start() {
        if (!this.running.getAndSet(true)) {
            this.executor.execute(() -> {
                while (this.running.get()) {
                    try (FileChannel fileChannel = FileChannel.open(Paths.get(this.path), StandardOpenOption.WRITE, StandardOpenOption.CREATE);
                         FileLock fileLock = fileChannel.tryLock()) {
                        if (fileLock.isValid()) {
                            this.takeLeadership();
                        }
                    } catch (Exception exception) {
                        try {
                            Thread.sleep(100L);
                        } catch (InterruptedException interruptedException) {
                            throw new RuntimeException(interruptedException);
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
                this.lossLeadership();
                this.executor.shutdown();
                this.executor.awaitTermination(5L, TimeUnit.SECONDS);
            } finally {
                this.executor.shutdownNow();
            }
        }
    }
}
