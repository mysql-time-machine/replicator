package com.booking.replication.applier.kafka;

import com.booking.replication.applier.Applier;
import com.booking.replication.applier.Seeker;
import com.booking.replication.augmenter.model.AugmentedEvent;
import com.booking.replication.augmenter.model.AugmentedEventHeader;
import com.booking.replication.augmenter.model.AugmentedEventTable;
import com.booking.replication.augmenter.model.AugmentedEventType;
import com.booking.replication.augmenter.model.ByteArrayAugmentedEventData;
import com.booking.replication.commons.checkpoint.Checkpoint;
import com.booking.replication.commons.services.ServicesControl;
import com.booking.replication.commons.services.ServicesProvider;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class KafkaTest {
    private static final String TOPIC_NAME = "replicator";
    private static final String GROUP_ID = "replicator";
    private static final int TOPIC_PARTITIONS = 3;
    private static final int TOPIC_REPLICAS = 1;

    private static AugmentedEvent[] events;
    private static AugmentedEvent lastEvent;
    private static ServicesControl servicesControl;

    private static Checkpoint getCheckpoint(int index) {
        return new Checkpoint(
                0,
                null,
                0,
                "PSEUDO_GTID",
                index
        );
    }

    private static AugmentedEvent getAugmentedEvent(int index) {
        byte[] data = new byte[100];

        ThreadLocalRandom.current().nextBytes(data);

        return new AugmentedEvent(
                new AugmentedEventHeader(
                        System.currentTimeMillis(),
                        KafkaTest.getCheckpoint(index),
                        AugmentedEventType.BYTE_ARRAY,
                        new AugmentedEventTable("DATABASE", "TABLE")
                ),
                new ByteArrayAugmentedEventData(data)
        );
    }

    @BeforeClass
    public static void before() {
        KafkaTest.events = new AugmentedEvent[3];

        for (int index  = 0; index < KafkaTest.events.length; index ++) {
            KafkaTest.events[index] = KafkaTest.getAugmentedEvent(index);
        }

        KafkaTest.lastEvent = KafkaTest.getAugmentedEvent(KafkaTest.events.length);
        KafkaTest.servicesControl = ServicesProvider.build(ServicesProvider.Type.CONTAINERS).startKafka(KafkaTest.TOPIC_NAME, KafkaTest.TOPIC_PARTITIONS, KafkaTest.TOPIC_REPLICAS);
    }

    @Test
    public void testApplier() throws IOException {
        Map<String, String> configuration = new HashMap<>();

        configuration.put(Applier.Configuration.TYPE, Applier.Type.KAFKA.name());
        configuration.put(KafkaApplier.Configuration.PARTITIONER, KafkaPartitioner.RANDOM.name());
        configuration.put(KafkaApplier.Configuration.TOPIC, KafkaTest.TOPIC_NAME);
        configuration.put(String.format("%s%s", KafkaApplier.Configuration.PRODUCER_PREFIX, "bootstrap.servers"), KafkaTest.servicesControl.getURL());

        try (Applier applier = Applier.build(configuration)) {
            for (AugmentedEvent event : KafkaTest.events) {
                applier.accept(event);
            }
        }
    }

    @Test
    public void testSeeker() {
        Map<String, String> configuration = new HashMap<>();

        configuration.put(Seeker.Configuration.TYPE, Seeker.Type.KAFKA.name());
        configuration.put(KafkaSeeker.Configuration.TOPIC, KafkaTest.TOPIC_NAME);
        configuration.put(String.format("%s%s", KafkaSeeker.Configuration.CONSUMER_PREFIX, "bootstrap.servers"), KafkaTest.servicesControl.getURL());
        configuration.put(String.format("%s%s", KafkaSeeker.Configuration.CONSUMER_PREFIX, "group.id"), KafkaTest.GROUP_ID);

        Seeker seeker = Seeker.build(configuration, KafkaTest.events[KafkaTest.events.length - 1].getHeader().getCheckpoint());

        for (AugmentedEvent event : KafkaTest.events) {
            assertNull(seeker.apply(event));
        }

        assertNotNull(seeker.apply(KafkaTest.lastEvent));
    }

    @AfterClass
    public static void after() {
        KafkaTest.servicesControl.close();
    }
}
