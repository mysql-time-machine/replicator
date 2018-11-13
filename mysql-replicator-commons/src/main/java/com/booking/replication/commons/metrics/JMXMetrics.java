package com.booking.replication.commons.metrics;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.jmx.JmxReporter;

import java.util.Map;

public class JMXMetrics extends Metrics<JmxReporter> {
    public JMXMetrics(Map<String, Object> configuration) {
        super(configuration);
    }

    private static JMXMetrics instance;

    public static JMXMetrics getInstance(Map<String, Object> configuration){
        if(instance == null){
            instance = new JMXMetrics(configuration);
        }
        return instance;
    }

    @Override
    protected JmxReporter getReporter(Map<String, Object> configuration, MetricRegistry registry) {
        JmxReporter reporter = JmxReporter.forRegistry(registry).build();

        reporter.start();

        return reporter;
    }
}
