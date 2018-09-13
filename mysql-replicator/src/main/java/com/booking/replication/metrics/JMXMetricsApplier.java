package com.booking.replication.metrics;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.jmx.JmxReporter;

import java.util.Map;

public class JMXMetricsApplier extends MetricsApplier<JmxReporter> {
    public JMXMetricsApplier(Map<String, Object> configuration) {
        super(configuration);
    }

    @Override
    protected JmxReporter getReporter(Map<String, Object> configuration, MetricRegistry registry) {
        JmxReporter reporter = JmxReporter.forRegistry(registry).build();

        reporter.start();

        return reporter;
    }
}
