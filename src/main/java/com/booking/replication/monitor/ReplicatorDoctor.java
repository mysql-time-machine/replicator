package com.booking.replication.monitor;

import com.codahale.metrics.Counting;
import org.slf4j.Logger;

/**
 * Able of making health assessments for the Replicator
 */
public class ReplicatorDoctor implements IReplicatorDoctor {

    private final String counterDescription;
    private final Counting globallyVisibleProgressCounter;
    private final Logger logger;
    private long lastObservedCounterValue;
    private Boolean hasReadTheCounter = false;

    public ReplicatorDoctor(Counting globallyVisibleProgressCounter, String counterDescription,
                            Logger logger)
    {
        if (globallyVisibleProgressCounter == null)
        {
            throw new IllegalArgumentException("globallyVisibleProgressCounter must be not null");
        }

        if (logger == null)
        {
            throw new IllegalArgumentException("logger must be not null");
        }

        this.counterDescription = counterDescription;
        this.globallyVisibleProgressCounter = globallyVisibleProgressCounter;
        this.logger = logger;
    }

    public ReplicatorHealthAssessment makeHealthAssessment() {

        if (!hasReadTheCounter)
        {
            this.lastObservedCounterValue = globallyVisibleProgressCounter.getCount();
            hasReadTheCounter = true;
            return ReplicatorHealthAssessment.Normal;
        }

        long currentCounterValue = globallyVisibleProgressCounter.getCount();

        if (currentCounterValue == lastObservedCounterValue) {

            String diagnosis = String.format("The replicator hasn't made any progress between consecutive observations (judging by %s)", counterDescription);

            logger.info(diagnosis);

            return new ReplicatorHealthAssessment(
                    false,
                    diagnosis);
        }
        else
        {
            this.lastObservedCounterValue = currentCounterValue;
            return ReplicatorHealthAssessment.Normal;
        }
    }
}
