package com.booking.replication.applier;

import com.booking.replication.Configuration;
import com.booking.replication.Metrics;
import com.booking.replication.augmenter.AugmentedRow;
import com.booking.replication.augmenter.AugmentedRowsEvent;
import com.booking.replication.augmenter.AugmentedSchemaChangeEvent;
import com.booking.replication.pipeline.PipelineOrchestrator;

import com.codahale.metrics.Counter;
import com.google.code.or.binlog.impl.event.FormatDescriptionEvent;
import com.google.code.or.binlog.impl.event.QueryEvent;
import com.google.code.or.binlog.impl.event.RotateEvent;
import com.google.code.or.binlog.impl.event.XidEvent;

import java.util.*;
import java.io.IOException;

import kafka.producer.KeyedMessage;
import kafka.javaapi.producer.Producer;
import kafka.producer.ProducerConfig;

import static com.codahale.metrics.MetricRegistry.name;

/**
 * Created by raynald on 08/06/16.
 */

public class KafkaApplier implements Applier {
    //    private static long totalEventsCounter = 0;
//    private static long totalRowsCounter = 0;
    private ProducerConfig config;
    private KeyedMessage<String, String> message;

    private final Producer<String, String> producer;
//    private static final HashMap<String, MutableLong> stats = new HashMap<>();

    private static final Counter messages = Metrics.registry.counter(name("Kafka", "messages"));

    public KafkaApplier(Configuration configuration) {
        // TODO: move to somewhere else
        long numOfEvents = 10000;
        String brokers = "kafka-202:9092";

        Properties props = new Properties();
        props.put("metadata.broker.list", brokers);
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("producer.type", "async");

        producer = new Producer<>(new ProducerConfig(props));
    }

    @Override
    public void applyAugmentedRowsEvent(AugmentedRowsEvent augmentedSingleRowEvent, PipelineOrchestrator caller) {
//        totalEventsCounter ++;
        // TODO: limit the number of events

        for (AugmentedRow row : augmentedSingleRowEvent.getSingleRowEvents()) {
            String tableName = row.getTableName();
            if (tableName != null) {
//                totalRowsCounter++;
                messages.inc();
            }
            String topic = row.getTableName();
            message = new KeyedMessage<>(topic, row.toJSON());
            producer.send(message);
//            System.out.println("One line has been sent to Kafka broker...");
        }
        producer.close();
    }

    @Override
    public void applyCommitQueryEvent(QueryEvent event) {

    }

    @Override
    public void applyXIDEvent(XidEvent event) {

    }

    @Override
    public void applyRotateEvent(RotateEvent event) {

    }

    @Override
    public void applyAugmentedSchemaChangeEvent(AugmentedSchemaChangeEvent augmentedSchemaChangeEvent, PipelineOrchestrator caller) {

    }

    @Override
    public void forceFlush() {

    }

    @Override
    public void resubmitIfThereAreFailedTasks() {

    }

    @Override
    public void applyFormatDescriptionEvent(FormatDescriptionEvent event) {

    }

    @Override
    public void waitUntilAllRowsAreCommitted() {

    }
}
