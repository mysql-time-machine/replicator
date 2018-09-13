package com.booking.replication.metrics;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Slf4jReporter;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.TimeUnit;

public class ConsoleMetricsApplier extends MetricsApplier<Slf4jReporter> {
    public ConsoleMetricsApplier(Map<String, Object> configuration) {
        super(configuration);
    }

    @Override
    protected Slf4jReporter getReporter(Map configuration, MetricRegistry registry) {
        Slf4jReporter reporter = Slf4jReporter.forRegistry(registry)
                .outputTo(LoggerFactory.getLogger("metrics"))
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.SECONDS)
                .build();

        reporter.start(1L, TimeUnit.MINUTES);

        return reporter;
    }
}
