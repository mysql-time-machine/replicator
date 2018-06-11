package com.booking.replication.applier.kafka;

import com.booking.replication.augmenter.model.AugmentedEvent;
import com.booking.replication.augmenter.model.QueryAugmentedEventData;

import java.util.concurrent.ThreadLocalRandom;

public enum KafkaPartitioner {
    TABLE_NAME {
        @Override
        public int partition(AugmentedEvent augmentedEvent, int totalPartitions) {
            if (QueryAugmentedEventData.class.isInstance(augmentedEvent.getData())) {
                return Math.abs(
                        QueryAugmentedEventData.class.cast(
                                augmentedEvent.getHeader()
                        ).getTable().hashCode()
                ) % totalPartitions;
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
