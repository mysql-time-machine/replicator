package com.booking.replication.applier.kafka;

import com.booking.replication.applier.EventApplier;
import com.booking.replication.applier.EventSeeker;
import com.booking.replication.model.Checkpoint;
import com.booking.replication.model.RawEvent;
import com.booking.replication.model.EventType;
import com.booking.replication.model.PseudoGTIDEventHeaderImplementation;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.MockProducer;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Date;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class KafkaRawEventTest {
    @Before
    public void before() {

    }

    @Test
    public void testApplier() throws InvocationTargetException, NoSuchMethodException, InstantiationException, IOException, IllegalAccessException {
        ObjectMapper mapper = new ObjectMapper();

        EventApplier applier = new KafkaEventApplier(
                new MockProducer<>(), "test", 10, KafkaEventPartitioner.RANDOM
        );

        applier.accept(RawEvent.build(
                mapper,
                new PseudoGTIDEventHeaderImplementation(
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        new Date().getTime(),
                        EventType.ROTATE,
                        new Checkpoint()
                ),
                "{\"binlogFilename\": \"binlog.0001\", \"binlogPosition\": 0}".getBytes()
        ));
    }

    @Test
    public void testSeeker() throws InvocationTargetException, NoSuchMethodException, InstantiationException, IOException, IllegalAccessException {
        ObjectMapper mapper = new ObjectMapper();

        RawEvent rawEvent0 = RawEvent.build(
                mapper,
                new PseudoGTIDEventHeaderImplementation(
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        new Date().getTime(),
                        EventType.ROTATE,
                        new Checkpoint(0, null, 0, "PSEUDO_GTID", 0)
                ),
                "{\"binlogFilename\": \"binlog.0001\", \"binlogPosition\": 0}".getBytes()
        );

        RawEvent rawEvent1 = RawEvent.build(
                mapper,
                new PseudoGTIDEventHeaderImplementation(
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        new Date().getTime(),
                        EventType.ROTATE,
                        new Checkpoint(0, null, 0, "PSEUDO_GTID", 1)
                ),
                "{\"binlogFilename\": \"binlog.0001\", \"binlogPosition\": 1}".getBytes()
        );

        RawEvent rawEvent2 = RawEvent.build(
                mapper,
                new PseudoGTIDEventHeaderImplementation(
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        new Date().getTime(),
                        EventType.ROTATE,
                        new Checkpoint(0, null, 0, "PSEUDO_GTID", 2)
                ),
                "{\"binlogFilename\": \"binlog.0001\", \"binlogPosition\": 2}".getBytes()
        );

        EventSeeker seeker = new KafkaEventSeeker(new Checkpoint(0, null, 0, "PSEUDO_GTID", 1));

        assertNull(seeker.apply(rawEvent0));
        assertNull(seeker.apply(rawEvent1));
        assertNotNull(seeker.apply(rawEvent2));
    }
}
