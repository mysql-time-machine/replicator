package com.booking.replication.applier.kafka;

import com.booking.replication.applier.EventSeeker;
import com.booking.replication.model.Checkpoint;
import com.booking.replication.model.Event;
import com.booking.replication.model.augmented.AugmentedEventHeader;
import com.booking.replication.model.augmented.AugmentedEventHeaderImplementation;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class KafkaEventSeeker implements EventSeeker {
    public interface Configuration extends KafkaEventApplier.Configuration {
        String GROUP_ID = "kafka.group.id";
    }

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final Checkpoint checkpoint;
    private final AtomicBoolean seeked;

    public KafkaEventSeeker(Checkpoint checkpoint) {
        this.checkpoint = checkpoint;
        this.seeked = new AtomicBoolean();
    }

    public KafkaEventSeeker(Map<String, String> configuration, Checkpoint checkpoint) {
        String bootstrapServers = configuration.get(Configuration.BOOTSTRAP_SERVERS);
        String groupId = configuration.get(Configuration.GROUP_ID);
        String topic = configuration.get(Configuration.TOPIC);

        Objects.requireNonNull(bootstrapServers, String.format("Configuration required: %s", Configuration.BOOTSTRAP_SERVERS));
        Objects.requireNonNull(groupId, String.format("Configuration required: %s", Configuration.GROUP_ID));
        Objects.requireNonNull(topic, String.format("Configuration required: %s", Configuration.TOPIC));

        this.checkpoint = this.geCheckpoint(checkpoint, bootstrapServers, groupId, topic);
        this.seeked = new AtomicBoolean();
    }

    private Checkpoint geCheckpoint(Checkpoint checkpoint, String bootstrapServers, String groupId, String topic) {
        Map<String, Object> configuration = new HashMap<>();

        configuration.put("bootstrap.servers", bootstrapServers);
        configuration.put("group.id", groupId);
        configuration.put("key.deserializer", ByteArrayDeserializer.class.getName());
        configuration.put("value.deserializer", ByteArrayDeserializer.class.getName());

        try (Consumer<byte[], byte[]> consumer = new KafkaConsumer<>(configuration)) {
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
                                consumerRecord.key(), AugmentedEventHeaderImplementation.class
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
    public Event apply(Event event) {
        if (this.seeked.get()) {
            return event;
        } else if (this.checkpoint == null || this.checkpoint.compareTo(AugmentedEventHeader.class.cast(event.getHeader()).getCheckpoint()) < 0) {
            this.seeked.set(true);
            return event;
        } else {
            return null;
        }
    }
}
