package com.booking.replication.commons.metrics;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.graphite.Graphite;
import com.codahale.metrics.graphite.GraphiteReporter;
import com.codahale.metrics.jvm.ClassLoadingGaugeSet;
import com.codahale.metrics.jvm.FileDescriptorRatioGauge;
import com.codahale.metrics.jvm.GarbageCollectorMetricSet;
import com.codahale.metrics.jvm.MemoryUsageGaugeSet;
import com.codahale.metrics.jvm.ThreadStatesGaugeSet;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

public class GraphiteMetrics extends Metrics<ScheduledReporter> {

    public interface Configuration {
        String GRAPHITE_NAMESPACE = "metrics.applier.graphite.namespace";
        String GRAPHITE_HOSTNAME = "metrics.applier.graphite.hostname";
        String GRAPHITE_PORT = "metrics.applier.graphite.port";
    }

    public GraphiteMetrics(Map<String, Object> configuration) {
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

        registry.register(MetricRegistry.name("jvm", "gc"), new GarbageCollectorMetricSet());
        registry.register(MetricRegistry.name("jvm", "threads"), new ThreadStatesGaugeSet());
        registry.register(MetricRegistry.name("jvm", "classes"), new ClassLoadingGaugeSet());
        registry.register(MetricRegistry.name("jvm", "fd"), new FileDescriptorRatioGauge());
        registry.register(MetricRegistry.name("jvm", "memory"), new MemoryUsageGaugeSet());

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
