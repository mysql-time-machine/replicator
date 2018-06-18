package com.booking.replication.metrics;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.graphite.Graphite;
import com.codahale.metrics.graphite.GraphiteReporter;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

public class GraphicMetricsApplier extends MetricsApplier<ScheduledReporter> {
    public interface Configuration {
        String GRAPHITE_NAMESPACE = "metrics.applier.graphite.namespace";
        String GRAPHITE_HOSTNAME = "metrics.applier.graphite.hostname";
        String GRAPHITE_PORT = "metrics.applier.graphite.port";
    }

    public GraphicMetricsApplier(Map<String, Object> configuration) {
        super(configuration);
    }

    @Override
    protected ScheduledReporter getReporter(Map<String, Object> configuration, MetricRegistry registry) {
        Object namespace = configuration.get(Configuration.GRAPHITE_NAMESPACE);
        Object hostname = configuration.get(Configuration.GRAPHITE_HOSTNAME);
        Object port = configuration.get(Configuration.GRAPHITE_PORT);

        Objects.requireNonNull(namespace, String.format("Configuration required: %s", Configuration.GRAPHITE_NAMESPACE));
        Objects.requireNonNull(hostname, String.format("Configuration required: %s", Configuration.GRAPHITE_HOSTNAME));
        Objects.requireNonNull(port, String.format("Configuration required: %s", Configuration.GRAPHITE_PORT));

        ScheduledReporter reporter = GraphiteReporter
                .forRegistry(registry)
                .prefixedWith(namespace.toString())
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.SECONDS)
                .build(new Graphite(new InetSocketAddress(hostname.toString(), Integer.parseInt(port.toString()))));

        reporter.start(1L, TimeUnit.MINUTES);

        return reporter;
    }
}