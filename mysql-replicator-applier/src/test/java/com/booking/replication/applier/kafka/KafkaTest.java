package com.booking.replication.applier.kafka;

import com.booking.replication.applier.Applier;
import com.booking.replication.applier.Seeker;
import com.booking.replication.augmenter.model.event.AugmentedEvent;
import com.booking.replication.augmenter.model.event.AugmentedEventHeader;
import com.booking.replication.augmenter.model.event.AugmentedEventType;
import com.booking.replication.augmenter.model.event.ByteArrayAugmentedEventData;
import com.booking.replication.commons.checkpoint.Binlog;
import com.booking.replication.commons.checkpoint.Checkpoint;
import com.booking.replication.commons.checkpoint.GTID;
import com.booking.replication.commons.checkpoint.GTIDType;
import com.booking.replication.commons.metrics.Metrics;
import com.booking.replication.commons.services.ServicesControl;
import com.booking.replication.commons.services.ServicesProvider;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class KafkaTest {
    private static final String TOPIC_NAME = "replicator";
    private static final String GROUP_ID = "replicator";
    private static final int TOPIC_PARTITIONS = 1;
    private static final int TOPIC_REPLICAS = 1;

    private static List<AugmentedEvent> events;
    private static AugmentedEvent lastEvent;
    private static ServicesControl servicesControl;

    private static Checkpoint getCheckpoint(int index) {
        return new Checkpoint(
                System.currentTimeMillis(),
                0,
                new GTID(
                        GTIDType.PSEUDO,
                        String.valueOf(index),
                        Byte.MAX_VALUE
                ),
                new Binlog(
                        null,
                        0
                )
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
                        "dbName",
                        "tableName"
                ),
                new ByteArrayAugmentedEventData(data)
        );
    }

    @BeforeClass
    public static void before() {
        KafkaTest.events = new ArrayList<>();

        for (int index  = 0; index < 3; index ++) {
            KafkaTest.events.add(KafkaTest.getAugmentedEvent(index));
        }

        KafkaTest.lastEvent = KafkaTest.getAugmentedEvent(KafkaTest.events.size());
        KafkaTest.servicesControl = ServicesProvider.build(ServicesProvider.Type.CONTAINERS).startKafka(KafkaTest.TOPIC_NAME, KafkaTest.TOPIC_PARTITIONS, KafkaTest.TOPIC_REPLICAS);
    }

    @Test
    public void testApplier() throws IOException {
        Map<String, Object> configuration = new HashMap<>();

        configuration.put(Applier.Configuration.TYPE, Applier.Type.KAFKA.name());
        configuration.put(KafkaApplier.Configuration.TOPIC, KafkaTest.TOPIC_NAME);
        configuration.put(String.format("%s%s", KafkaApplier.Configuration.PRODUCER_PREFIX, ProducerConfig.BOOTSTRAP_SERVERS_CONFIG), KafkaTest.servicesControl.getURL());

        Metrics.build(configuration, null);
        try (Applier applier = Applier.build(configuration)) {
            applier.apply(KafkaTest.events);
        }
    }

    @Test
    public void testSeeker() {
        Map<String, Object> configuration = new HashMap<>();

        configuration.put(Seeker.Configuration.TYPE, Seeker.Type.KAFKA.name());
        configuration.put(KafkaSeeker.Configuration.TOPIC, KafkaTest.TOPIC_NAME);
        configuration.put(String.format("%s%s", KafkaSeeker.Configuration.CONSUMER_PREFIX, ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG), KafkaTest.servicesControl.getURL());
        configuration.put(String.format("%s%s", KafkaSeeker.Configuration.CONSUMER_PREFIX, ConsumerConfig.GROUP_ID_CONFIG), KafkaTest.GROUP_ID);

        Metrics.build(configuration, null);
        Seeker seeker = Seeker.build(configuration);

        seeker.seek(KafkaTest.events.get(KafkaTest.events.size() - 1).getHeader().getCheckpoint());

        assertNull(seeker.apply(KafkaTest.events));
        assertNotNull(seeker.apply(Collections.singletonList(KafkaTest.lastEvent)));
    }

    @AfterClass
    public static void after() {
        KafkaTest.servicesControl.close();
    }
}
