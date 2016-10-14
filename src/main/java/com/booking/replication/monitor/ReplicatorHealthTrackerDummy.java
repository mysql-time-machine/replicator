package com.booking.replication.monitor;

/**
 * Created by mdutikov on 10/13/2016.
 */
public class ReplicatorHealthTrackerDummy implements IReplicatorHealthTracker {

    @Override
    public ReplicatorHealthAssessment getLastHealthAssessment() {
        return ReplicatorHealthAssessment.Normal;
    }

    @Override
    public void start() {
    }

    @Override
    public void stop() {
    }
}
