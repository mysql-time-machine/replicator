package com.booking.replication.monitor;

import java.util.Timer;
import java.util.TimerTask;

/**
 * Makes periodic health assessments for the Replicator.
 *
 */
public class ReplicatorHealthTracker implements IReplicatorHealthTracker {

    private class ReevaluateProgressTask extends TimerTask {
        public void run() {
            synchronized (criticalSection) {
                if (!isRunning) {
                    timer.cancel();
                    return;
                }

                lastHealthAssessment = replicatorDoctor.makeHealthAssessment();

                timer.schedule(new ReevaluateProgressTask(), millisecondsBetweenHealthReevaluations);
            }
        }
    }

    private final IReplicatorDoctor replicatorDoctor;
    private final Object criticalSection = new Object();
    private Boolean isRunning = false;
    private Timer timer;
    private long millisecondsBetweenHealthReevaluations;
    private ReplicatorHealthAssessment lastHealthAssessment;

    public ReplicatorHealthTracker(
            IReplicatorDoctor replicatorDoctor,
            long secondsBetweenHealthReevaluations)
    {
        this.replicatorDoctor = replicatorDoctor;
        if (secondsBetweenHealthReevaluations <= 0)
        {
            throw new IllegalArgumentException("Expecting a positive number for the # of seconds between reevaluations");
        }

        this.millisecondsBetweenHealthReevaluations = secondsBetweenHealthReevaluations * 1000;
    }

    public void start()
    {
        synchronized (criticalSection)
        {
            if (isRunning)
            {
                throw new IllegalStateException("Already running");
            }

            this.lastHealthAssessment = this.replicatorDoctor.makeHealthAssessment();

            //not a daemon thread
            timer = new Timer(false);

            timer.schedule(new ReevaluateProgressTask(), millisecondsBetweenHealthReevaluations);

            isRunning = true;
        }
    }

    @Override
    public void stop() {
        synchronized (criticalSection) {
            if (!isRunning)
            {
                throw new IllegalStateException("Not running: cannot stop");
            }

            timer.cancel();

            isRunning = false;
        }
    }

    @Override
    public ReplicatorHealthAssessment getLastHealthAssessment()
    {
        synchronized (criticalSection)
        {
            if (lastHealthAssessment == null)
            {
                throw new IllegalStateException("No health check assessments made so far. Have you started the tracker?");
            }

            return lastHealthAssessment;
        }
    }
}
