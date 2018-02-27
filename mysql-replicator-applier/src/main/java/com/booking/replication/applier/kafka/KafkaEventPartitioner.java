package com.booking.replication.applier.kafka;

import com.booking.replication.model.Event;
import com.booking.replication.model.EventData;
import com.booking.replication.model.TableNameEventData;

import java.util.concurrent.ThreadLocalRandom;

public enum KafkaEventPartitioner {
    TABLE_NAME {
        @Override
        public int partition(Event event, int totalPartitions) {
            EventData data = event.getData();

            if (data instanceof TableNameEventData) {
                return Math.abs(TableNameEventData.class.cast(data).getTableName().hashCode()) % totalPartitions;
            } else {
                throw new IllegalArgumentException(String.format("illegal event data type: %s", data.getClass().getInterfaces()[0].getName()));
            }
        }
    },
    RANDOM {
        @Override
        public int partition(Event event, int totalPartitions) {
            return ThreadLocalRandom.current().nextInt(totalPartitions);
        }
    };

    public abstract int partition(Event event, int totalPartitions);
}
