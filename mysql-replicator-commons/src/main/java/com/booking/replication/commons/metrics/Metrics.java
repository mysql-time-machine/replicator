package com.booking.replication.commons.metrics;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Reporter;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public abstract class Metrics<CloseableReporter extends Closeable & Reporter> implements Closeable {
    public enum Type {
        CONSOLE {
            @Override
            protected Metrics<?> newInstance(Map<String, Object> configuration) {
                return ConsoleMetrics.getInstance(configuration);
            }
        },
        JMX {
            @Override
            protected Metrics<?> newInstance(Map<String, Object> configuration) {
                return JMXMetrics.getInstance(configuration);
            }
        },
        GRAPHITE {
            @Override
            protected Metrics<?> newInstance(Map<String, Object> configuration) {
                return GraphicMetrics.getInstance(configuration);
            }
        };

        protected abstract Metrics<?> newInstance(Map<String, Object> configuration);
    }

    public interface Configuration {
        String TYPE = "metrics.applier.type";
        String PATH = "metrics.applier.delay.path";
    }

    private static final String BASE_PATH = "events";

    private final MetricRegistry registry;
    private final CloseableReporter reporter;
    private final String delayName;

    public Metrics(Map<String, Object> configuration) {
        this.registry = new MetricRegistry();
        this.reporter = this.getReporter(configuration, this.registry);
        this.delayName = MetricRegistry.name(
                Metrics.BASE_PATH,
                this.getList(configuration.getOrDefault(Configuration.PATH, "delay")).toArray(new String[0])
        );
    }

    public MetricRegistry getRegistry() {
        return registry;
    }

    @SuppressWarnings("unchecked")
    private List<String> getList(Object object) {
        if (List.class.isInstance(object)) {
            return (List<String>) object;
        } else {
            return Collections.singletonList(object.toString());
        }
    }

    @Override
    public void close() throws IOException  {
        this.reporter.close();
    }

    protected abstract CloseableReporter getReporter(Map<String, Object> configuration, MetricRegistry registry);

    public static Metrics<?> build(Map<String, Object> configuration) {
        return Metrics.Type.valueOf(
                configuration.getOrDefault(Configuration.TYPE, Type.CONSOLE.name()).toString()
        ).newInstance(configuration);
    }
}
