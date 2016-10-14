package com.booking.replication.monitor;

/**
 * Responds with ReplicatorHealthAssessment.Normal until an actual tracker is provided, to delegate
 * work to it. After it's started, the delegation begins.
 */
public class ReplicatorHealthTrackerProxy implements IReplicatorHealthTracker {

    private IReplicatorHealthTracker realTracker;
    private Boolean hasBeenStarted = false;
    private final Object criticalSection = new Object();

    public void setTrackerImplementation(IReplicatorHealthTracker tracker) {
        if (tracker == null) {
            throw new IllegalArgumentException("tracker must not be null");
        }

        synchronized (criticalSection)
        {
            if (this.realTracker != null)
            {
                throw new IllegalStateException("Can only set the tracker once");
            }

            this.realTracker = tracker;
        }
    }

    @Override
    public ReplicatorHealthAssessment getLastHealthAssessment() {

        synchronized (criticalSection)
        {
            if (!hasBeenStarted)
            {
                return ReplicatorHealthAssessment.Normal;
            }

            return realTracker.getLastHealthAssessment();
        }
    }

    @Override
    public void start() {
        synchronized (criticalSection) {
            if (realTracker == null)
            {
                throw new IllegalStateException("Set the tracker doing real work first");
            }

            realTracker.start();
            hasBeenStarted = true;
        }
    }

    @Override
    public void stop() {
        synchronized (criticalSection) {

            if (realTracker == null) {
                throw new IllegalStateException("Set the tracker doing real work first");
            }

            realTracker.stop();
        }
    }
}