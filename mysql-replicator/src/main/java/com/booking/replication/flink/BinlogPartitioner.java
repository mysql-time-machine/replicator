package com.booking.replication.flink;

import com.booking.replication.augmenter.model.event.AugmentedEvent;
import com.booking.replication.augmenter.model.event.AugmentedEventTransaction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.functions.KeySelector;

import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

public class BinlogPartitioner {

    public static Partitioner<UUID> getPartitioner(int tasks) {

        return  (Partitioner<UUID>) (transactionUUID, totalPartitions) -> {

            if (transactionUUID != null) {

                Long tmp = transactionUUID.getMostSignificantBits() & Integer.MAX_VALUE;
                System.out.println("partition => " +  Math.toIntExact(Long.remainderUnsigned(tmp, totalPartitions)));

                return Math.toIntExact(Long.remainderUnsigned(tmp, totalPartitions));

            } else {
                return ThreadLocalRandom.current().nextInt(tasks);
            }
        };

    }

    public static KeySelector<AugmentedEvent, UUID> getKeySelector() {

        return (KeySelector<AugmentedEvent, UUID>) event -> {

            if (event.getHeader().getEventTransaction() != null) {

                AugmentedEventTransaction transaction = event.getHeader().getEventTransaction();

                UUID transactionUUID = UUID.fromString(transaction.getIdentifier());

                return transactionUUID;

            } else {
                return null;
            }

        };
    }
}
