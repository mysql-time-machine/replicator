package com.booking.replication.applier;

import static com.codahale.metrics.MetricRegistry.name;

import com.booking.replication.Configuration;
import com.booking.replication.Metrics;
import com.booking.replication.augmenter.AugmentedRow;
import com.booking.replication.augmenter.AugmentedRowsEvent;
import com.booking.replication.augmenter.AugmentedSchemaChangeEvent;
import com.booking.replication.pipeline.PipelineOrchestrator;

import com.google.code.or.binlog.impl.event.FormatDescriptionEvent;
import com.google.code.or.binlog.impl.event.QueryEvent;
import com.google.code.or.binlog.impl.event.RotateEvent;
import com.google.code.or.binlog.impl.event.XidEvent;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import org.apache.commons.lang.mutable.MutableLong;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by raynald on 08/06/16.
 */

public class KafkaApplier implements Applier {
    private final com.booking.replication.Configuration replicatorConfiguration;
    private static long totalEventsCounter = 0;
    private static long totalRowsCounter = 0;
    private static long totalOutlierCounter = 0;
    private Properties producerProps;
    private Properties consumerProps;
    private KafkaProducer<String, String> producer;
    private KafkaConsumer<String, String> consumer;
    private ProducerRecord<String, String> message;
    private static List<String> topicList;
    private String schemaName;

    private AtomicBoolean exceptionFlag = new AtomicBoolean(false);
    private static final Meter kafka_messages = Metrics.registry.meter(name("Kafka", "producerToBroker"));
    private static final Counter exception_counters = Metrics.registry.counter(name("Kafka", "exceptionCounter"));
    private static final Timer closureTimer = Metrics.registry.timer(name("Kafka", "producerCloseTimer"));
    private static final HashMap<String, MutableLong> stats = new HashMap<>();
    private static final HashMap<Long, Long> lastCommited = new HashMap<>();
    private ConsumerRecords<String, String> records;

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaApplier.class);

    public KafkaApplier(Configuration configuration) throws IOException {
        replicatorConfiguration = configuration;

        /**
         * kafka.producer.Producer provides the ability to batch multiple produce requests (producer.type=async),
         * before serializing and dispatching them to the appropriate kafka broker partition. The size of the batch
         * can be controlled by a few config parameters. As events enter a queue, they are buffered in a queue, until
         * either queue.time or batch.size is reached. A background thread (kafka.producer.async.ProducerSendThread)
         * dequeues the batch of data and lets the kafka.producer.DefaultEventHandler serialize and send the data to
         * the appropriate kafka broker partition.
         */

        // Below is the new version of producer configuration
        producerProps = new Properties();
        producerProps.put("bootstrap.servers", configuration.getKafkaBrokerAddress());
        producerProps.put("acks", "all"); // Default 1
        producerProps.put("retries", 30); // Default value: 0
        producerProps.put("batch.size", 5384); // Default value: 16384
        producerProps.put("linger.ms", 20); // Default 0, Artificial delay
        producerProps.put("buffer.memory", 33554432); // Default value: 33554432
        producerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.put("metric.reporters", "com.booking.replication.applier.KafkaMetricsCollector");
        producerProps.put("request.timeout.ms", 100000);

        // Consumer configuration
        consumerProps = new Properties();
        consumerProps.put("bootstrap.servers", configuration.getKafkaBrokerAddress());
        consumerProps.put("group.id", "producer_wingman");
        // consumerProps.put("auto.offset.reset", "latest");
        consumerProps.put("enable.auto.commit", "false");
        consumerProps.put("auto.commit.interval.ms", "1000");
        consumerProps.put("session.timeout.ms", "30000");
        consumerProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        producer = new KafkaProducer<>(producerProps);
        consumer = new KafkaConsumer<>(consumerProps);
        topicList = configuration.getKafkaTopicList();
        schemaName = configuration.getReplicantSchemaName();
        getLastPosition();
        LOGGER.info("Size of last commited hashmap: " + lastCommited.size());
        for (Long i: lastCommited.keySet()) {
            LOGGER.info("Show lastCommited partition: " + i.toString() + " -> uniqueID: " + lastCommited.get(i).toString());
        }
    }

    public void getLastPosition() throws IOException {
        for (int i = 0;i < producer.partitionsFor(schemaName).size();i ++) {
            TopicPartition partition = new TopicPartition(schemaName, i);
            consumer.assign(Arrays.asList(partition));
            consumer.seek(partition, consumer.position(partition) - 1);
            records = consumer.poll(1000); // TODO: change to parameter
            for (ConsumerRecord<String, String> record: records) {
                String cutString = record.value().substring(record.value().indexOf("uniqueID"));
                // TODO: JsonDeserialization doesn't work for BinlogEventV4Header
                // Now extracting uuid from String by index instead
                Long uuid = Long.parseLong(cutString.substring("uniqueID': ".length(), cutString.indexOf(",") - 1));
                if (!lastCommited.keySet().contains(i) || lastCommited.get(Long.valueOf(i)) < uuid) {
                    lastCommited.put(Long.valueOf(i), uuid);
                }
            }
        }
    }

    @Override
    public void applyAugmentedRowsEvent(AugmentedRowsEvent augmentedSingleRowEvent, PipelineOrchestrator caller) {
        long singleRowsCounter = 0;
        int partitionNum;
        int numberOfPartition = producer.partitionsFor(schemaName).size();
        Long rowUniqueID;

        totalEventsCounter ++;

        for (AugmentedRow row : augmentedSingleRowEvent.getSingleRowEvents()) {
            if (exceptionFlag.get()) {
                throw new RuntimeException("Producer has problem with sending messages, could be a connection issue");
            }
            if (row.getTableName() == null) {
                LOGGER.error("tableName not exists");
                throw new RuntimeException("tableName does not exist");
            }

            String topic = row.getTableName();
            if (topicList.contains(topic)) {
                totalRowsCounter++;
                rowUniqueID = Long.valueOf(totalEventsCounter * 100 + singleRowsCounter ++);
                partitionNum = (row.getTableName().hashCode() % numberOfPartition + numberOfPartition) % numberOfPartition;
                if (!lastCommited.keySet().contains(Long.valueOf(partitionNum))
                        || rowUniqueID.compareTo(lastCommited.get(Long.valueOf(partitionNum))) > 0) {
                    row.setUniqueID(rowUniqueID.toString());
                    message = new ProducerRecord<>(
                            schemaName,
                            partitionNum,
                            String.valueOf(row.getEventV4Header().getPosition()),
                            row.toJson());
                    producer.send(message, new Callback() {
                        @Override
                        public void onCompletion(RecordMetadata recordMetadata, Exception sendException) {
                            if (sendException != null) {
                                LOGGER.error("Error producing to Kafka broker");
                                sendException.printStackTrace();
                                exceptionFlag.set(true);
                                exception_counters.inc();
                            }
                        }
                    });
                    if (totalRowsCounter % 500 == 0) {
                        LOGGER.debug("500 lines have been sent to Kafka broker...");
                    }
                    kafka_messages.mark();
                }
            } else {
                totalOutlierCounter ++;
                if (totalOutlierCounter % 500 == 0) {
                    LOGGER.warn("Over 500 non-supported topics, for example: " + topic);
                }
            }
        }
    }

    @Override
    public void applyCommitQueryEvent(QueryEvent event) {

    }

    @Override
    public void applyXidEvent(XidEvent event) {

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
    public void applyFormatDescriptionEvent(FormatDescriptionEvent event) {

    }

    @Override
    public void waitUntilAllRowsAreCommitted(RotateEvent event) {
        final Timer.Context context = closureTimer.time();
        producer.close();
        context.stop();
        LOGGER.warn("New producer");
        producer = new KafkaProducer<>(producerProps);
    }
}
