package com.booking.replication;

import com.booking.replication.checkpoints.LastVerifiedBinlogFile;
import com.booking.replication.checkpoints.SafeCheckPoint;
import com.booking.replication.coordinator.CoordinatorInterface;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by rmirica on 31/05/16.
 */
public class Coordinator {
    private static final Logger LOGGER = LoggerFactory.getLogger(Coordinator.class);

    private static CoordinatorInterface implementation;
    private static Configuration configuration;

    public static void setImplementation(CoordinatorInterface impl) {
        implementation = impl;
    }

    public static void setConfiguration(Configuration conf) { configuration = conf; }

    public static CoordinatorInterface getImplementation() {
        return implementation;
    }

    public static void saveCheckpointMarker(LastVerifiedBinlogFile marker) throws Exception {
        implementation.storeSafeCheckPoint(marker);
    }

    public static SafeCheckPoint getCheckpointMarker() {
        SafeCheckPoint cp = implementation.getSafeCheckPoint();
        if(cp == null) {
            new LastVerifiedBinlogFile(configuration.getReplicantDBServerID(), configuration.getStartingBinlogFileName());
        }
        try {
            LOGGER.info(String.format("Got checkpoint: %s", implementation.serialize(cp)));
        } catch (Exception e) {
            e.printStackTrace();
        }
        return cp;
    }
}
