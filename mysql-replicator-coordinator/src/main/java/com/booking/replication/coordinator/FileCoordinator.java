package com.booking.replication.coordinator;

import com.booking.replication.commons.checkpoint.Checkpoint;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Map;

public class FileCoordinator extends Coordinator {
    public interface Configuration {
        String LEADERSHIP_PATH = "file.leadership.path";
    }

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final String path;

    private FileChannel fileChannel;
    private FileLock fileLock;

    public FileCoordinator(Map<String, Object> configuration) {
        this.path = configuration.getOrDefault(Configuration.LEADERSHIP_PATH, "/tmp/leadership.coordinator").toString();
    }

    @Override
    public void saveCheckpoint(String path, Checkpoint checkpoint) throws IOException {
        if (checkpoint != null) {
            Files.write(Paths.get(path), FileCoordinator.MAPPER.writeValueAsBytes(checkpoint));
        }
    }

    @Override
    public Checkpoint loadCheckpoint(String path) throws IOException {
        try {
            byte[] bytes = Files.readAllBytes(Paths.get(path));

            if (bytes.length > 0) {
                return FileCoordinator.MAPPER.readValue(bytes, Checkpoint.class);
            } else {
                return null;
            }
        } catch (NoSuchFileException exception) {
            return null;
        }
    }

    @Override
    public void start() {
        if (this.fileChannel == null) {
            try {
                this.fileChannel = FileChannel.open(Paths.get(this.path), StandardOpenOption.WRITE, StandardOpenOption.CREATE);
            } catch (IOException exception) {
                throw new UncheckedIOException(exception);
            }
        }

        super.start();
    }

    @Override
    public void awaitLeadership() {
        try {
            while (this.fileChannel != null && this.fileLock == null) {
                try {
                    this.fileLock = this.fileChannel.lock();
                } catch (OverlappingFileLockException exception) {
                    Thread.sleep(1000L);
                }
            }
        } catch (IOException exception) {
            throw new UncheckedIOException(exception);
        } catch (InterruptedException exception) {
            throw new RuntimeException(exception);
        }
    }

    @Override
    public final void stop() {
        super.stop();

        if (this.fileLock != null) {
            try {
                this.fileLock.release();
                this.fileLock = null;
            } catch (IOException exception) {
                throw new UncheckedIOException(exception);
            }
        }

        if (this.fileChannel != null) {
            try {
                this.fileChannel.close();
                this.fileChannel = null;
            } catch (IOException exception) {
                throw new UncheckedIOException(exception);
            }
        }
    }
}
