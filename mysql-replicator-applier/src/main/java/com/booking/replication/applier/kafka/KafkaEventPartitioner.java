package com.booking.replication.applier.kafka;

import com.booking.replication.augmenter.model.AugmentedEvent;
import com.booking.replication.supplier.model.RawEvent;
import com.booking.replication.supplier.model.EventData;
import com.booking.replication.supplier.model.TableNameEventData;

import java.util.concurrent.ThreadLocalRandom;

public enum KafkaEventPartitioner {
    TABLE_NAME {
        @Override
        public int partition(AugmentedEvent augmentedEvent, int totalPartitions) {
            return Math.abs(augmentedEvent.getHeader().getTableName().hashCode()) % totalPartitions;
        }
    },
    RANDOM {
        @Override
        public int partition(AugmentedEvent augmentedEvent, int totalPartitions) {
            return ThreadLocalRandom.current().nextInt(totalPartitions);
        }
    };

    public abstract int partition(AugmentedEvent augmentedEvent, int totalPartitions);
}
