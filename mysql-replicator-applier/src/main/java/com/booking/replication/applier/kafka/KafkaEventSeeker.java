package com.booking.replication.applier.kafka;

import com.booking.replication.applier.EventSeeker;
import com.booking.replication.augmenter.model.AugmentedEvent;
import com.booking.replication.supplier.model.Checkpoint;
import com.booking.replication.supplier.model.PseudoGTIDEventHeader;
import com.booking.replication.augmenter.model.PseudoGTIDEventHeaderImplementation;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class KafkaEventSeeker implements EventSeeker {
    public interface Configuration extends KafkaEventApplier.Configuration {
        String CONSUMER_PREFIX = "kafka.consumer";
    }

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final Checkpoint checkpoint;
    private final AtomicBoolean seeked;

    public KafkaEventSeeker(Checkpoint checkpoint) {
        this.checkpoint = checkpoint;
        this.seeked = new AtomicBoolean();
    }

    public KafkaEventSeeker(Map<String, String> configuration, Checkpoint checkpoint) {
        String topic = configuration.get(Configuration.TOPIC);

        Objects.requireNonNull(topic, String.format("Configuration required: %s", Configuration.TOPIC));

        this.checkpoint = this.geCheckpoint(checkpoint, configuration.entrySet().stream().filter(entry -> entry.getKey().startsWith(Configuration.CONSUMER_PREFIX)).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)), topic);
        this.seeked = new AtomicBoolean();
    }

    private Checkpoint geCheckpoint(Checkpoint checkpoint, Map<String, Object> configuration, String topic) {
        try (Consumer<byte[], byte[]> consumer = new KafkaConsumer<>(configuration, new ByteArrayDeserializer(), new ByteArrayDeserializer())) {
            Checkpoint lastCheckpoint = checkpoint;

            List<TopicPartition> topicPartitions = consumer.partitionsFor(topic).stream().map(partitionInfo -> new TopicPartition(partitionInfo.topic(), partitionInfo.partition())).collect(Collectors.toList());

            for (TopicPartition topicPartition : topicPartitions) {
                consumer.assign(Collections.singletonList(topicPartition));

                long endOffset = consumer.endOffsets(Collections.singletonList(topicPartition)).get(topicPartition);

                if (endOffset > 0) {
                    consumer.seek(topicPartition, endOffset - 1);

                    ConsumerRecords<byte[], byte[]> consumerRecords = consumer.poll(100);

                    for (ConsumerRecord<byte[], byte[]> consumerRecord : consumerRecords) {
                        Checkpoint currentCheckpoint = KafkaEventSeeker.MAPPER.readValue(
                                consumerRecord.key(), PseudoGTIDEventHeaderImplementation.class
                        ).getCheckpoint();

                        if (lastCheckpoint != null && lastCheckpoint.compareTo(currentCheckpoint) < 0) {
                            lastCheckpoint = currentCheckpoint;
                        }
                    }
                }
            }

            return lastCheckpoint;
        } catch (IOException exception) {
            throw new UncheckedIOException(exception);
        }
    }

    @Override
    public AugmentedEvent apply(AugmentedEvent rawEvent) {
        if (this.seeked.get()) {
            return rawEvent;
        } else if (this.checkpoint == null || this.checkpoint.compareTo(PseudoGTIDEventHeader.class.cast(rawEvent.getHeader()).getCheckpoint()) < 0) {
            this.seeked.set(true);
            return rawEvent;
        } else {
            return null;
        }
    }
}
