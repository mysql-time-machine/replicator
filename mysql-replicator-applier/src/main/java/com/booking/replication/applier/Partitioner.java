package com.booking.replication.applier;

import com.booking.replication.augmenter.model.AugmentedEvent;
import com.booking.replication.augmenter.model.AugmentedEventTable;
import com.booking.replication.augmenter.model.TableAugmentedEventData;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.BiFunction;

public interface Partitioner extends BiFunction<AugmentedEvent, Integer, Integer>, Closeable {
    enum Type {
        TABLE_NAME {
            @Override
            protected Partitioner newInstance(Map<String, Object> configuration) {
                return (augmentedEvent, totalPartitions) -> {
                    if (TableAugmentedEventData.class.isInstance(augmentedEvent.getData())) {
                        AugmentedEventTable eventTable = TableAugmentedEventData.class.cast(augmentedEvent.getData()).getEventTable();

                        if (eventTable != null) {
                            return Math.abs(eventTable.toString().hashCode()) % totalPartitions;
                        } else {
                            return ThreadLocalRandom.current().nextInt(totalPartitions);
                        }
                    } else {
                        return ThreadLocalRandom.current().nextInt(totalPartitions);
                    }
                };
            }
        },
        RANDOM {
            @Override
            protected Partitioner newInstance(Map<String, Object> configuration) {
                return (augmentedEvent, totalPartitions) -> ThreadLocalRandom.current().nextInt(totalPartitions);
            }
        },
        NONE {
            @Override
            protected  Partitioner newInstance(Map<String, Object> configuration) {
                return (augmentedEvent, totalPartitions) -> 0;
            }
        };

        protected abstract Partitioner newInstance(Map<String, Object> configuration);
    }

    interface Configuration {
        String TYPE = "partitioner.type";
    }

    @Override
    default void close() throws IOException {
    }

    static Partitioner build(Map<String, Object> configuration) {
        return Partitioner.Type.valueOf(
                configuration.getOrDefault(Configuration.TYPE, Type.RANDOM.name()).toString()
        ).newInstance(configuration);
    }
}
