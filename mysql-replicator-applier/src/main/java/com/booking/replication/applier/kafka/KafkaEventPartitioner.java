package com.booking.replication.applier.kafka;

import com.booking.replication.supplier.model.RawEvent;
import com.booking.replication.supplier.model.EventData;
import com.booking.replication.supplier.model.TableNameEventData;

import java.util.concurrent.ThreadLocalRandom;

public enum KafkaEventPartitioner {
    TABLE_NAME {
        @Override
        public int partition(RawEvent rawEvent, int totalPartitions) {
            EventData data = rawEvent.getData();

            if (data instanceof TableNameEventData) {
                return Math.abs(TableNameEventData.class.cast(data).getTableName().hashCode()) % totalPartitions;
            } else {
                throw new IllegalArgumentException(String.format("illegal rawEvent data type: %s", data.getClass().getInterfaces()[0].getName()));
            }
        }
    },
    RANDOM {
        @Override
        public int partition(RawEvent rawEvent, int totalPartitions) {
            return ThreadLocalRandom.current().nextInt(totalPartitions);
        }
    };

    public abstract int partition(RawEvent rawEvent, int totalPartitions);
}
