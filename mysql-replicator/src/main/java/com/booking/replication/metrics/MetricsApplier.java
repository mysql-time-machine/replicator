package com.booking.replication.metrics;

import com.booking.replication.augmenter.model.event.AugmentedEvent;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Reporter;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public abstract class MetricsApplier<CloseableReporter extends Closeable & Reporter> implements Function<AugmentedEvent, Boolean>, Closeable {
    public enum Type {
        CONSOLE {
            @Override
            protected MetricsApplier<?> newInstance(Map<String, Object> configuration) {
                return new ConsoleMetricsApplier(configuration);
            }
        },
        JMX {
            @Override
            protected MetricsApplier<?> newInstance(Map<String, Object> configuration) {
                return new JMXMetricsApplier(configuration);
            }
        },
        GRAPHITE {
            @Override
            protected MetricsApplier<?> newInstance(Map<String, Object> configuration) {
                return new GraphicMetricsApplier(configuration);
            }
        };

        protected abstract MetricsApplier<?> newInstance(Map<String, Object> configuration);
    }

    public interface Configuration {
        String TYPE = "metrics.applier.type";
        String PATH = "metrics.applier.delay.path";
    }

    private static final String BASE_PATH = "events";

    private final MetricRegistry registry;
    private final CloseableReporter reporter;
    private final String delayName;

    public MetricsApplier(Map<String, Object> configuration) {
        this.registry = new MetricRegistry();
        this.reporter = this.getReporter(configuration, this.registry);
        this.delayName = MetricRegistry.name(
                MetricsApplier.BASE_PATH,
                this.getList(configuration.getOrDefault(Configuration.PATH, "delay")).toArray(new String[0])
        );
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
    public Boolean apply(AugmentedEvent event) {
        this.registry.histogram(this.delayName).update(System.currentTimeMillis() - event.getHeader().getTimestamp());

        return true;
    }

    @Override
    public void close() throws IOException  {
        this.reporter.close();
    }

    protected abstract CloseableReporter getReporter(Map<String, Object> configuration, MetricRegistry registry);

    public static MetricsApplier<?> build(Map<String, Object> configuration) {
        return MetricsApplier.Type.valueOf(
                configuration.getOrDefault(Configuration.TYPE, Type.CONSOLE.name()).toString()
        ).newInstance(configuration);
    }
}
