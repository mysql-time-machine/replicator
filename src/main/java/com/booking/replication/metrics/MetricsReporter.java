package com.booking.replication.metrics;

import com.booking.replication.util.Duration;

import com.codahale.metrics.ScheduledReporter;

/**
 * Created by rmirica on 06/06/16.
 */
public abstract class MetricsReporter {

    public abstract ScheduledReporter getReporter();

    public abstract Duration getFrequency();

    public void start() {
        getReporter().start(
                getFrequency().getQuantity(),
                getFrequency().getUnit()
        );
    }

}
