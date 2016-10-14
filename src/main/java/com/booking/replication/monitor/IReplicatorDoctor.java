package com.booking.replication.monitor;

/**
 * Able of making health assessments for the Replicator
 */
public interface IReplicatorDoctor {
    ReplicatorHealthAssessment makeHealthAssessment();
}
