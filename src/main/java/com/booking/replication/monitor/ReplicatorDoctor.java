package com.booking.replication.monitor;

import com.codahale.metrics.Counting;
import org.slf4j.Logger;

/**
 * Able of making health assessments for the Replicator
 */
public class ReplicatorDoctor implements IReplicatorDoctor {

    private final String counterDescription;
    private final Counting globallyImpactingEventsCounter;
    private final Logger logger;
    private final Counting numberOfInterestingEventsObserved;
    private long lastExternalWorkCounterValue;
    private long lastObservedNumberOfInterestingEvents;
    private Boolean hasReadTheCounter = false;

    /**
     * @param eventsPushedToWorldCounter - the counter which helps measure the work the Replicator pushes to the outside world
     * @param counterDescription
     * @param logger
     * @param interestingEventsObservedCounter - the counter which shows how many interesting events the Replicator has observed (the # of events
     *                                          that have reached the Applier) in the databases being monitored
     */
    public ReplicatorDoctor(
            Counting eventsPushedToWorldCounter,
            String counterDescription,
            Logger logger,
            Counting interestingEventsObservedCounter)
    {
        if (eventsPushedToWorldCounter == null)
        {
            throw new IllegalArgumentException("eventsPushedToWorldCounter must be not null");
        }

        if (interestingEventsObservedCounter == null)
        {
            throw new IllegalArgumentException("interestingEventsObservedCounter must be not null");
        }

        if (logger == null)
        {
            throw new IllegalArgumentException("logger must be not null");
        }

        this.counterDescription = counterDescription;
        this.globallyImpactingEventsCounter = eventsPushedToWorldCounter;
        this.logger = logger;
        this.numberOfInterestingEventsObserved = interestingEventsObservedCounter;
    }

    public ReplicatorHealthAssessment makeHealthAssessment() {

        if (!hasReadTheCounter)
        {
            this.lastExternalWorkCounterValue = globallyImpactingEventsCounter.getCount();
            this.lastObservedNumberOfInterestingEvents = numberOfInterestingEventsObserved.getCount();
            hasReadTheCounter = true;
            return ReplicatorHealthAssessment.Normal;
        }

        long externalWorkCounter = globallyImpactingEventsCounter.getCount();
        long numberOfInterestingEvents = numberOfInterestingEventsObserved.getCount();

        if ((numberOfInterestingEvents != lastObservedNumberOfInterestingEvents) && (externalWorkCounter == lastExternalWorkCounterValue)) {

            String diagnosis = String.format(
                    "The replicator hasn't made any progress between consecutive observations. Two samples of \"%s\" - current: %d, previous: %d; Two samples for the # of interesting events - current: %d, previous: %d;",
                    counterDescription,
                    externalWorkCounter,
                    lastExternalWorkCounterValue,
                    numberOfInterestingEvents,
                    lastObservedNumberOfInterestingEvents);

            logger.info(diagnosis);

            return new ReplicatorHealthAssessment(
                    false,
                    diagnosis);
        }
        else
        {
            this.lastExternalWorkCounterValue = externalWorkCounter;
            this.lastObservedNumberOfInterestingEvents = numberOfInterestingEvents;
            return ReplicatorHealthAssessment.Normal;
        }
    }
}
