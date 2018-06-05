package com.booking.replication.augmenter;

import com.booking.replication.augmenter.active.schema.ActiveSchemaAugmenter;
import com.booking.replication.augmenter.model.AugmentedEvent;
import com.booking.replication.supplier.model.RawEvent;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.function.Function;

public interface Augmenter extends Function<RawEvent, AugmentedEvent>, Closeable {
    enum Type {
        NONE {
            @Override
            protected Augmenter newInstance(Map<String, String> configuration)
            {
                return event -> null;
            }
        },
        ACTIVE_SCHEMA {
            @Override
            protected Augmenter newInstance(Map<String, String> configuration) {
                return new ActiveSchemaAugmenter(configuration);
            }
        };

        protected abstract Augmenter newInstance(Map<String, String> configuration);
    }

    interface Configuration {
        String TYPE = "augmenter.type";
    }

    @Override
    default void close() throws IOException {
    }

    static Augmenter build(Map<String, String> configuration) {
        return Augmenter.Type.valueOf(
                configuration.getOrDefault(Configuration.TYPE, Type.NONE.name())
        ).newInstance(configuration);
    }
}