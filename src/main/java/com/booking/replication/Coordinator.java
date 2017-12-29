package com.booking.replication;

import com.booking.replication.checkpoints.LastCommittedPositionCheckpoint;
import com.booking.replication.coordinator.CoordinatorInterface;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class provides coordination functions based on a CoordinatorInterface implementation,
 * currently Zookeeper and File coordinators are supported.
 */
public class Coordinator {
    private static final Logger LOGGER = LoggerFactory.getLogger(Coordinator.class);

    private static CoordinatorInterface implementation;

    public static void setImplementation(CoordinatorInterface impl) {
        implementation = impl;
    }

    public static CoordinatorInterface getImplementation() {
        return implementation;
    }

    public static void saveCheckpointMarker(LastCommittedPositionCheckpoint marker) throws Exception {
        implementation.storeSafeCheckPoint(marker);
    }

    /**
     * Fetch the latest checkpoint marker.
     *
     * @return Checkpoint marker
     */
    public static LastCommittedPositionCheckpoint getSafeCheckpoint() {
        LastCommittedPositionCheckpoint cp = implementation.getSafeCheckPoint();
        try {
            LOGGER.info(String.format("Got checkpoint: %s", implementation.serialize(cp)));
        } catch (Exception e) {
            LOGGER.error("Could not get safe checkpoint marker", e);
            System.exit(1);
        }
        return cp;
    }

    public static void onLeaderElection(Runnable runnable) throws InterruptedException {
        implementation.onLeaderElection(runnable);
    }
}
