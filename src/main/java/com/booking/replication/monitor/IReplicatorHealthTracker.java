package com.booking.replication.monitor;

/**
 * Makes periodic health assessments for the Replicator
 */
public interface IReplicatorHealthTracker {
    ReplicatorHealthAssessment getLastHealthAssessment();

    void start();

    void stop();
}
