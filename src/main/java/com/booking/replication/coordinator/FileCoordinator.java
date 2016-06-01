package com.booking.replication.coordinator;

import com.booking.replication.Configuration;
import com.booking.replication.checkpoints.LastVerifiedBinlogFile;
import com.booking.replication.checkpoints.SafeCheckPoint;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Created by rmirica on 01/06/16.
 */
public class FileCoordinator implements CoordinatorInterface {
    private static final Logger LOGGER = LoggerFactory.getLogger(FileCoordinator.class);

    private final Configuration configuration;

    private SafeCheckPoint checkPoint;
    private final String checkPointPath;

    public FileCoordinator(Configuration configuration) {
        this.configuration = configuration;
        checkPointPath  = configuration.getMetadataFile();
    }

    @Override
    public boolean onLeaderElection(Runnable callback) throws InterruptedException {
        callback.run();
        callback.wait();
        return true;
    }

    private ObjectMapper mapper = new ObjectMapper();

    @Override
    public String serialize(SafeCheckPoint checkPoint) throws JsonProcessingException {
        return mapper.writeValueAsString(checkPoint);
    }

    @Override
    public void storeSafeCheckPoint(SafeCheckPoint safeCheckPoint) throws Exception {
        checkPoint = safeCheckPoint;
        try {
            String serialized = serialize(checkPoint);

            LOGGER.info(String.format("Serialized checkpoint: %s", serialized));

            Path tempFile = Files.createTempFile(null, ".replicator");
            LOGGER.info(String.format("Creating file: %s", tempFile.getFileName()));

            BufferedWriter writer = Files.newBufferedWriter(tempFile, Charset.forName("UTF-8"));
            try {
                writer.write(serialized, 0, serialized.length());
            } catch (IOException e) {
                LOGGER.error(String.format("Got an error while trying to write: %s (%s)", tempFile.getFileName(), e.getMessage()));
                e.printStackTrace();
                throw e;
            }
            writer.flush();
            writer.close();

            if(!tempFile.toFile().renameTo(new File(checkPointPath))){
                throw new RuntimeException(String.format("Failed to rename the metadata file to: %s", checkPointPath));
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }

    @Override
    public SafeCheckPoint getSafeCheckPoint() {
        try {
            return mapper.readValue(Files.newInputStream(Paths.get(checkPointPath)), LastVerifiedBinlogFile.class);
        } catch (JsonProcessingException e) {
            LOGGER.error(String.format("Failed to deserialize checkpoint data. %s", e.getMessage()));
            e.printStackTrace();
        } catch (Exception e) {
            LOGGER.error(String.format("Got an error while reading metadata from file: %s (%s)", checkPointPath, e.getMessage()));
            e.printStackTrace();
        }

        return null;
    }

}
