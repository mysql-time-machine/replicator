package com.booking.replication.applier;

import com.booking.replication.augmenter.model.event.AugmentedEvent;
import com.booking.replication.augmenter.model.event.AugmentedEventTransaction;
import com.booking.replication.augmenter.model.event.TableAugmentedEventData;
import com.booking.replication.augmenter.model.schema.FullTableName;

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
                return (event, totalPartitions) -> {
                    if (TableAugmentedEventData.class.isInstance(event.getData())) {
                        FullTableName eventTable = TableAugmentedEventData.class.cast(event.getData()).getMetadata().getEventTable();

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
                        /*
                            There exists an edge-case in which the hashCode for the value of getIdentifier is
                            Integer.MIN_VALUE, which will result in Math.abs returning the same value (MIN_VALUE when
                            positive is 1 larger than MAX_VALUE, causing it to overflow back to MIN_VALUE). Will need
                            to add some metrics to this to ensure it's not common enough that we're skewing the distribution
                            across the queues
                         */

                        int hashCode = Math.abs( transaction.getIdentifier().hashCode() );
                        int part = ( hashCode == Integer.MIN_VALUE ? Integer.MAX_VALUE : hashCode ) % totalPartitions;

                        return part;
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
