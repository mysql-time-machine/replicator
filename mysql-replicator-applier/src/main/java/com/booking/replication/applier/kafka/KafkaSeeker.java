package com.booking.replication.applier.kafka;

import com.booking.replication.applier.Seeker;
import com.booking.replication.augmenter.model.AugmentedEvent;
import com.booking.replication.augmenter.model.AugmentedEventHeader;
import com.booking.replication.commons.checkpoint.Checkpoint;

import com.booking.replication.commons.map.MapFilter;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class KafkaSeeker implements Seeker {
    public interface Configuration {
        String TOPIC = "kafka.topic";
        String CONSUMER_PREFIX = "kafka.consumer.";
    }

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final String topic;
    private final Map<String, Object> configuration;
    private final AtomicReference<Checkpoint> checkpoint;
    private final AtomicBoolean seeked;

    public KafkaSeeker(Map<String, String> configuration) {
        String topic = configuration.get(Configuration.TOPIC);

        Objects.requireNonNull(topic, String.format("Configuration required: %s", Configuration.TOPIC));

        this.topic = topic;
        this.configuration = new MapFilter(configuration).filter(Configuration.CONSUMER_PREFIX);
        this.checkpoint = new AtomicReference<>();
        this.seeked = new AtomicBoolean();
    }

    @Override
    public Checkpoint seek(Checkpoint checkpoint) {
        try (Consumer<byte[], byte[]> consumer = new KafkaConsumer<>(this.configuration, new ByteArrayDeserializer(), new ByteArrayDeserializer())) {
            Checkpoint lastCheckpoint = checkpoint;

            consumer.subscribe(Arrays.asList(this.topic));
            consumer.poll(100L);

            Map<TopicPartition, Long>  endOffsetMap = consumer.endOffsets(consumer.assignment());

            for (Map.Entry<TopicPartition, Long> endOffsetEntry : endOffsetMap.entrySet()) {
                long endOffset = endOffsetEntry.getValue();

                if (endOffset > 0) {
                    consumer.seek(endOffsetEntry.getKey(), endOffset - 1);

                    ConsumerRecords<byte[], byte[]> consumerRecords = consumer.poll(100);

                    for (ConsumerRecord<byte[], byte[]> consumerRecord : consumerRecords) {
                        Checkpoint currentCheckpoint = KafkaSeeker.MAPPER.readValue(
                                consumerRecord.key(), AugmentedEventHeader.class
                        ).getCheckpoint();

                        if (lastCheckpoint != null && lastCheckpoint.compareTo(currentCheckpoint) < 0) {
                            lastCheckpoint = currentCheckpoint;
                        }
                    }
                }
            }

            this.checkpoint.set(lastCheckpoint);
            this.seeked.set(false);

            return lastCheckpoint;
        } catch (IOException exception) {
            throw new UncheckedIOException(exception);
        }
    }

    @Override
    public AugmentedEvent apply(AugmentedEvent augmentedEvent) {
        if (this.seeked.get()) {
            return augmentedEvent;
        } else if (this.checkpoint.get() == null || this.checkpoint.get().compareTo(augmentedEvent.getHeader().getCheckpoint()) < 0) {
            this.seeked.set(true);
            return augmentedEvent;
        } else {
            return null;
        }
    }
}
