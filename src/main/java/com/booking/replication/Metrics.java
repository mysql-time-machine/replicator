package com.booking.replication;

import com.booking.replication.metrics.GraphiteReporter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.jvm.*;

import static com.codahale.metrics.MetricRegistry.name;

/**
 * This class provides facilities for using the Dropwizard-Metrics library.
 */
public class Metrics {
    public static MetricRegistry registry = new MetricRegistry();

    public static void setRegistry(MetricRegistry reg) {
        registry = reg;
    }

    /**
     * Start metric reporters.
     */
    public static void startReporters(Configuration conf) {
        registry.register(name("jvm", "gc"), new GarbageCollectorMetricSet());
        registry.register(name("jvm", "threads"), new ThreadStatesGaugeSet());
        registry.register(name("jvm", "classes"), new ClassLoadingGaugeSet());
        registry.register(name("jvm", "fd"), new FileDescriptorRatioGauge());
        registry.register(name("jvm", "memory"), new MemoryUsageGaugeSet());

        for (String reporter: conf.getMetricReporters().keySet()) {
            switch (reporter) {
                case "graphite":
                    new GraphiteReporter(conf).start();
                    break;
                case "console":
                    new com.booking.replication.metrics.ConsoleReporter(conf).start();
                    break;
                default:
                    break;
            }
        }
    }

}


