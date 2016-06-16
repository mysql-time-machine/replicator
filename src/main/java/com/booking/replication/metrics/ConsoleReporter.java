package com.booking.replication.metrics;

import com.booking.replication.Configuration;
import com.booking.replication.Metrics;
import com.booking.replication.util.Duration;

import com.codahale.metrics.ScheduledReporter;

import java.util.concurrent.TimeUnit;

/**
 * This class provides a Console Reporter.
 */
public class ConsoleReporter extends MetricsReporter {

    private static final String type = "console";

    private Duration frequency = Duration.parse("10 seconds");

    private com.codahale.metrics.ConsoleReporter reporter;

    /**
     * Start a metrics console reporter.
     */
    public ConsoleReporter(Configuration conf) {
        frequency = conf.getReportingFrequency();

        reporter = com.codahale.metrics.ConsoleReporter.forRegistry(Metrics.registry)
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .build();
    }

    @Override
    public ScheduledReporter getReporter() {
        return reporter;
    }

    @Override
    public Duration getFrequency() {
        return frequency;
    }
}
