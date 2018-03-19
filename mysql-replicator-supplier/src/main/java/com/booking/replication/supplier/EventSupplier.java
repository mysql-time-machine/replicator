package com.booking.replication.supplier;

import com.booking.replication.model.Checkpoint;
import com.booking.replication.model.Event;
import com.booking.replication.supplier.kafka.KafkaSupplier;
import com.booking.replication.supplier.mysql.binlog.BinaryLogSupplier;

import java.io.IOException;
import java.util.Map;
import java.util.function.Consumer;

public interface EventSupplier {
    enum Type {
        BINLOG {
            @Override
            public EventSupplier newInstance(Map<String, String> configuration, Checkpoint checkpoint) {
                return new BinaryLogSupplier(configuration, checkpoint);
            }
        },
        KAFKA {
            @Override
            public EventSupplier newInstance(Map<String, String> configuration, Checkpoint checkpoint) {
                return new KafkaSupplier(configuration, checkpoint);
            }
        };

        public abstract EventSupplier newInstance(Map<String, String> configuration, Checkpoint checkpoint);
    }

    interface Configuration {
        String TYPE = "supplier.type";
    }

    void onEvent(Consumer<Event> consumer);

    void start() throws IOException;

    void stop() throws IOException;

    @SuppressWarnings("unchecked")
    static EventSupplier build(Map<String, String> configuration, Checkpoint checkpoint) {
        return Type.valueOf(
                configuration.getOrDefault(Configuration.TYPE, Type.BINLOG.name())
        ).newInstance(configuration, checkpoint);
    }
}
