package com.booking.replication;

import com.booking.replication.metrics.GraphiteReporter;
import com.codahale.metrics.*;
import com.codahale.metrics.jvm.*;

import java.util.concurrent.ConcurrentHashMap;

import static com.codahale.metrics.MetricRegistry.name;


public class Metrics {
    public static MetricRegistry registry = new MetricRegistry();

    public static void setRegistry(MetricRegistry reg) {
        registry = reg;
    }

    public static class PerTableMetricsHash extends ConcurrentHashMap<String, PerTableMetrics>{

        private final String prefix;

        public PerTableMetricsHash(String prefix) {
            super();
            this.prefix = prefix;
        }

        public PerTableMetrics getOrCreate(String key) {
            putIfAbsent(key, new Metrics.PerTableMetrics(prefix, key));
            return get(key);
        }
    }

    public static class PerTableMetrics {
        public final Counter inserted;
        public final Counter processed;
        public final Counter deleted;
        public final Counter updated;
        public final Counter committed;

        public PerTableMetrics(String prefix, String tableName) {
            inserted    = Metrics.registry.counter(name(prefix, tableName, "inserted"));
            processed   = Metrics.registry.counter(name(prefix, tableName, "processed"));
            deleted     = Metrics.registry.counter(name(prefix, tableName, "deleted"));
            updated     = Metrics.registry.counter(name(prefix, tableName, "updated"));
            committed   = Metrics.registry.counter(name(prefix, tableName, "committed"));
        }
    }

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


