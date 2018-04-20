package com.booking.replication.applier;

import com.booking.replication.applier.cassandra.CassandraEventApplier;
import com.booking.replication.applier.console.ConsoleEventApplier;
import com.booking.replication.applier.hbase.HBaseEventApplier;
import com.booking.replication.applier.kafka.KafkaEventApplier;
import com.booking.replication.model.RawEvent;

import java.io.Closeable;
import java.util.Map;
import java.util.function.Consumer;

public interface EventApplier extends Consumer<RawEvent>, Closeable {
    enum Type {
        CONSOLE {
            @Override
            public EventApplier newInstance(Map<String, String> configuration) {
                return new ConsoleEventApplier(configuration);
            }
        },
        HBASE {
            @Override
            public EventApplier newInstance(Map<String, String> configuration) {
                return new HBaseEventApplier(configuration);
            }
        },
        KAFKA {
            @Override
            public EventApplier newInstance(Map<String, String> configuration) {
                return new KafkaEventApplier(configuration);
            }
        },
        CASSANDRA {
            @Override
            public EventApplier newInstance(Map<String, String> configuration) {
                return new CassandraEventApplier(configuration);
            }
        };

        public abstract EventApplier newInstance(Map<String, String> configuration);
    }

    interface Configuration {
        String TYPE = "applier.type";
    }

    static EventApplier build(Map<String, String> configuration) {
        return Type.valueOf(
                configuration.getOrDefault(Configuration.TYPE, Type.CONSOLE.name())
        ).newInstance(configuration);
    }
}
