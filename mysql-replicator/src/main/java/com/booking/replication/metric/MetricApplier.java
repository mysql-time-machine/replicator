package com.booking.replication.metric;

import com.booking.replication.augmenter.model.AugmentedEvent;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.function.Consumer;

public interface MetricApplier extends Consumer<AugmentedEvent>, Closeable {
    enum Type {
        NONE {
            @Override
            protected MetricApplier newInstance(Map<String, Object> configuration) {
                return (event) -> {
                };
            }
        };

        protected abstract MetricApplier newInstance(Map<String, Object> configuration);
    }

    interface Configuration {
        String TYPE = "metric.applier.type";
    }

    @Override
    default void close() throws IOException {
    }

    static MetricApplier build(Map<String, Object> configuration) {
        return MetricApplier.Type.valueOf(
                configuration.getOrDefault(Configuration.TYPE, Type.NONE.name()).toString()
        ).newInstance(configuration);
    }
}
