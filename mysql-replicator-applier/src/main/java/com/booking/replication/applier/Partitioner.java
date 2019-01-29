package com.booking.replication.applier;

import com.booking.replication.augmenter.model.event.AugmentedEvent;
import com.booking.replication.augmenter.model.schema.FullTableName;
import com.booking.replication.augmenter.model.event.AugmentedEventTransaction;
import com.booking.replication.augmenter.model.event.TableAugmentedEventData;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.BiFunction;

public interface Partitioner extends BiFunction<AugmentedEvent, Integer, Integer>, Closeable {
    enum Type {
        TABLE_NAME {
            @Override
            protected Partitioner newInstance(Map<String, Object> configuration) {
                return (event, totalPartitions) -> {
                    if (TableAugmentedEventData.class.isInstance(event.getData())) {
                        FullTableName eventTable = TableAugmentedEventData.class.cast(event.getData()).getEventTable();

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
        XXID {
            @Override
            protected Partitioner newInstance(Map<String, Object> configuration) {
                return (event, totalPartitions) -> {
                    if (event.getHeader().getEventTransaction() != null) {
                        AugmentedEventTransaction transaction = event.getHeader().getEventTransaction();
                        return Integer.remainderUnsigned( (int) transaction.getXXID(), totalPartitions );
                    } else {
                        return ThreadLocalRandom.current().nextInt(totalPartitions);
                    }
                };
            }
        },
        TRID {
            @Override
            protected Partitioner newInstance(Map<String, Object> configuration) {

                return (event, totalPartitions) -> {

                    if (event.getHeader().getEventTransaction() != null) {
                        AugmentedEventTransaction transaction = event.getHeader().getEventTransaction();
                        long tmp = UUID.fromString(transaction.getIdentifier()).getMostSignificantBits() & Integer.MAX_VALUE;
                        return Math.toIntExact(Long.remainderUnsigned(tmp, totalPartitions));
                    } else {
                        return ThreadLocalRandom.current().nextInt(totalPartitions);
                    }

                };
            }
        },
        RANDOM {
            @Override
            protected Partitioner newInstance(Map<String, Object> configuration) {
                return (event, totalPartitions) -> ThreadLocalRandom.current().nextInt(totalPartitions);
            }
        },
        NONE {
            @Override
            protected  Partitioner newInstance(Map<String, Object> configuration) {
                return (event, totalPartitions) -> 0;
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
                configuration.getOrDefault(Configuration.TYPE, Type.TRID.name()).toString()
        ).newInstance(configuration);
    }
}
