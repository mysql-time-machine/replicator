package com.booking.replication.applier.kafka;

import com.booking.replication.augmenter.model.AugmentedEvent;

import java.util.concurrent.ThreadLocalRandom;

public enum KafkaPartitioner {
    TABLE_NAME {
        @Override
        public int partition(AugmentedEvent augmentedEvent, int totalPartitions) {
            if (augmentedEvent.getHeader().getEventTable() != null) {
                return Math.abs(augmentedEvent.getHeader().getEventTable().getName().hashCode()) % totalPartitions;
            } else {
                return KafkaPartitioner.RANDOM.partition(augmentedEvent, totalPartitions);
            }
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
