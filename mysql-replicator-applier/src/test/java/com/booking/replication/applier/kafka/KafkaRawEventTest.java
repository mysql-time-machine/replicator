//package com.booking.replication.applier.kafka;
//
//import com.booking.replication.applier.EventApplier;
//import com.booking.replication.applier.EventSeeker;
//import com.booking.replication.augmenter.model.AugmentedEvent;
//import com.booking.replication.commons.checkpoint.Checkpoint;
//import com.booking.replication.supplier.model.RawEvent;
//import com.booking.replication.supplier.model.RawEventType;
//import com.booking.replication.augmenter.model.PseudoGTIDEventHeaderImplementation;
//import com.fasterxml.jackson.databind.ObjectMapper;
//import org.apache.kafka.clients.producer.MockProducer;
//import org.junit.Before;
//import org.junit.Test;
//
//import java.io.IOException;
//import java.lang.reflect.InvocationTargetException;
//import java.util.Date;
//
//import static org.junit.Assert.assertNotNull;
//import static org.junit.Assert.assertNull;
//
//public class KafkaRawEventTest {
//    @Before
//    public void before() {
//
//    }
//
//    @Test
//    public void testApplier() throws InvocationTargetException, NoSuchMethodException, InstantiationException, IOException, IllegalAccessException {
//        ObjectMapper mapper = new ObjectMapper();
//
//        EventApplier applier = new KafkaEventApplier(
//                new MockProducer<>(), "test", 10, KafkaEventPartitioner.RANDOM
//        );
//
//        // TODO: this does not work currently
//        //  -> replace with
//        //      AugmentedEvent augEvent = Augmenter.augment(rawEvent)
//        //      applier.accept(augEvent)
//        applier.accept(
//                RawEvent.build(
//                    mapper,
//                    new PseudoGTIDEventHeaderImplementation(
//                            0,
//                            0,
//                            0,
//                            0,
//                            0,
//                            0,
//                            new Date().getTime(),
//                            RawEventType.ROTATE,
//                            new Checkpoint()
//                    ),
//                    "{\"binlogFilename\": \"binlog.0001\", \"binlogPosition\": 0}".getBytes()
//                )
//        );
//    }
//
//    @Test
//    public void testSeeker() throws InvocationTargetException, NoSuchMethodException, InstantiationException, IOException, IllegalAccessException {
//        ObjectMapper mapper = new ObjectMapper();
//
//        RawEvent rawEvent0 = AugmentedEvent.build(
//                mapper,
//                new PseudoGTIDEventHeaderImplementation(
//                        0,
//                        0,
//                        0,
//                        0,
//                        0,
//                        0,
//                        new Date().getTime(),
//                        RawEventType.ROTATE,
//                        new Checkpoint(0, null, 0, "PSEUDO_GTID", 0)
//                ),
//                "{\"binlogFilename\": \"binlog.0001\", \"binlogPosition\": 0}".getBytes()
//        );
//
//        AugmentedEvent rawEvent1 = AugmentedEvent.build(
//                mapper,
//                new PseudoGTIDEventHeaderImplementation(
//                        0,
//                        0,
//                        0,
//                        0,
//                        0,
//                        0,
//                        new Date().getTime(),
//                        RawEventType.ROTATE,
//                        new Checkpoint(0, null, 0, "PSEUDO_GTID", 1)
//                ),
//                "{\"binlogFilename\": \"binlog.0001\", \"binlogPosition\": 1}".getBytes()
//        );
//
//        RawEvent rawEvent2 = AugmentedEvent.build(
//                mapper,
//                new PseudoGTIDEventHeaderImplementation(
//                        0,
//                        0,
//                        0,
//                        0,
//                        0,
//                        0,
//                        new Date().getTime(),
//                        RawEventType.ROTATE,
//                        new Checkpoint(0, null, 0, "PSEUDO_GTID", 2)
//                ),
//                "{\"binlogFilename\": \"binlog.0001\", \"binlogPosition\": 2}".getBytes()
//        );
//
//        EventSeeker seeker = new KafkaEventSeeker(new Checkpoint(0, null, 0, "PSEUDO_GTID", 1));
//
//        assertNull(seeker.apply(rawEvent0));
//        assertNull(seeker.apply(rawEvent1));
//        assertNotNull(seeker.apply(rawEvent2));
//    }
//}
