package com.booking.replication.flink;

import com.booking.replication.augmenter.model.event.AugmentedEvent;
import com.booking.replication.augmenter.model.event.AugmentedEventTransaction;
import com.booking.replication.augmenter.model.event.TableAugmentedEventData;
import com.booking.replication.augmenter.model.schema.FullTableName;
import org.apache.flink.api.common.functions.Partitioner;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

public interface BinlogEventFlinkPartitioner extends Partitioner<AugmentedEvent> {

    enum Type {

        TABLE_NAME {

            @Override
            protected Partitioner<AugmentedEvent> newInstance(Map<String, Object> configuration) {
                return new Partitioner<AugmentedEvent>() {
                    @Override
                    public int partition(AugmentedEvent event, int totalPartitions) {

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
                    }
                };
            }
        },

        XXID {
            @Override
            protected Partitioner<AugmentedEvent> newInstance(Map<String, Object> configuration) {
                return new Partitioner<AugmentedEvent>() {
                    @Override
                    public int partition(AugmentedEvent event, int totalPartitions) {
                        if (event.getHeader().getEventTransaction() != null) {
                            AugmentedEventTransaction transaction = event.getHeader().getEventTransaction();
                            return Integer.remainderUnsigned((int) transaction.getXXID(), totalPartitions);
                        } else {
                            return ThreadLocalRandom.current().nextInt(totalPartitions);
                        }
                    }
                };
            }
        },

        TRID {
            @Override
            protected Partitioner<AugmentedEvent> newInstance(Map<String, Object> configuration) {
                return new Partitioner<AugmentedEvent>() {
                    @Override
                    public int partition(AugmentedEvent event, int totalPartitions) {

                        if (event.getHeader().getEventTransaction() != null) {
                            AugmentedEventTransaction transaction = event.getHeader().getEventTransaction();
                            long tmp = UUID.fromString(transaction.getIdentifier()).getMostSignificantBits() & Integer.MAX_VALUE;
                            return Math.toIntExact(Long.remainderUnsigned(tmp, totalPartitions));
                        } else {
                            return ThreadLocalRandom.current().nextInt(totalPartitions);
                        }

                    }
                };
            }
        },

        RANDOM {
            @Override
            protected Partitioner<AugmentedEvent> newInstance(Map<String, Object> configuration) {
                return new Partitioner<AugmentedEvent>() {
                    @Override
                    public int partition(AugmentedEvent event, int totalPartitions) {
                        return ThreadLocalRandom.current().nextInt(totalPartitions);
                    }
                };
            }
        },

        NONE {
            @Override
            protected Partitioner<AugmentedEvent> newInstance(Map<String, Object> configuration) {
                return new Partitioner<AugmentedEvent>() {
                    @Override
                    public int partition(AugmentedEvent event, int totalPartitions) {
                        return 0;
                    }
                };
            }
        };

        protected abstract Partitioner<AugmentedEvent> newInstance(Map<String, Object> configuration);
    }

    interface Configuration {
        String TYPE = "partitioner.type";
    }

    static Partitioner<AugmentedEvent> build(Map<String, Object> configuration) {
        return BinlogEventFlinkPartitioner.Type.valueOf(
                configuration.getOrDefault(
                        BinlogEventFlinkPartitioner.Configuration.TYPE,
                        BinlogEventFlinkPartitioner.Type.TRID.name()).toString()
        ).newInstance(configuration);
    }
}

