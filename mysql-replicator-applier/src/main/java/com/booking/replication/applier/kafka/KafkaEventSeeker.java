package com.booking.replication.applier.kafka;

import com.booking.replication.applier.EventSeeker;
import com.booking.replication.model.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InvalidPartitionsException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.reflect.InvocationTargetException;
import java.util.*;
import java.util.stream.Collectors;

public class KafkaEventSeeker implements EventSeeker, Comparator<Event> {
    public interface Configuration extends KafkaEventApplier.Configuration {
        String GROUP_ID = "kafka.group.id";
    }

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final KafkaEventPartitioner partitioner;
    private final Event[] partitionLast;
    private final BitSet partitionSeeked;
    private boolean seeked;

    public KafkaEventSeeker(KafkaEventPartitioner partitioner, Event[] partitionLast) {
        this.partitioner = partitioner;
        this.partitionLast = partitionLast;
        this.partitionSeeked = new BitSet(this.partitionLast.length);
        this.seeked = false;
    }

    public KafkaEventSeeker(Map<String, String> configuration) {
        String bootstrapServers = configuration.get(Configuration.BOOTSTRAP_SERVERS);
        String groupId = configuration.get(Configuration.GROUP_ID);
        String topic = configuration.get(Configuration.TOPIC);
        String partitioner = configuration.get(Configuration.PARTITIONER);

        Objects.requireNonNull(bootstrapServers, String.format("Configuration required: %s", Configuration.BOOTSTRAP_SERVERS));
        Objects.requireNonNull(groupId, String.format("Configuration required: %s", Configuration.GROUP_ID));
        Objects.requireNonNull(topic, String.format("Configuration required: %s", Configuration.TOPIC));
        Objects.requireNonNull(partitioner, String.format("Configuration required: %s", Configuration.PARTITIONER));

        this.partitioner = KafkaEventPartitioner.valueOf(partitioner);
        this.partitionLast = this.getPartitionLast(bootstrapServers, groupId, topic);
        this.partitionSeeked = new BitSet(this.partitionLast.length);
        this.seeked = false;
    }

    private Event[] getPartitionLast(String bootstrapServers, String groupId, String topic) {
        Map<String, Object> configuration = new HashMap<>();

        configuration.put("bootstrap.servers", bootstrapServers);
        configuration.put("group.id", groupId);
        configuration.put("key.deserializer", ByteArrayDeserializer.class.getName());
        configuration.put("value.deserializer", ByteArrayDeserializer.class.getName());

        try (Consumer<byte[], byte[]> consumer = new KafkaConsumer<>(configuration)) {
            List<TopicPartition> topicPartitions = consumer.partitionsFor(topic).stream().map(partitionInfo -> new TopicPartition(partitionInfo.topic(), partitionInfo.partition())).collect(Collectors.toList());
            Event[] partitionLast = new Event[topicPartitions.stream().mapToInt(TopicPartition::partition).max().orElseThrow(() -> new InvalidPartitionsException("partitions not found")) + 1];

            for (TopicPartition topicPartition : topicPartitions) {
                consumer.assign(Collections.singletonList(topicPartition));
                Map<TopicPartition, Long> endOffsets = consumer.endOffsets(Collections.singletonList(topicPartition));
                consumer.seek(topicPartition, endOffsets.get(topicPartition));

                ConsumerRecords<byte[], byte[]> consumerRecords = consumer.poll(100);

                for (ConsumerRecord<byte[], byte[]> consumerRecord : consumerRecords) {
                    partitionLast[consumerRecord.partition()] = Event.build(KafkaEventSeeker.MAPPER, consumerRecord.key(), consumerRecord.value());
                }
            }

            return partitionLast;
        } catch (IOException exception) {
            throw new UncheckedIOException(exception);
        } catch (ReflectiveOperationException exception) {
            throw new RuntimeException(exception);
        }
    }

    @Override
    public Event apply(Event event) {
        if (this.seeked) {
            return event;
        } else {
            int partition = this.partitioner.partition(event, this.partitionLast.length);

            if (this.partitionLast[partition] == null || this.compare(this.partitionLast[partition], event) < 0) {
                this.partitionSeeked.set(partition, true);
                this.seeked = this.partitionSeeked.cardinality() == this.partitionSeeked.length();

                return event;
            } else {
                return null;
            }
        }
    }

    @Override
    public int compare(Event event1, Event event2) {
        return event1.toString().compareTo(event2.toString());
    }
}
