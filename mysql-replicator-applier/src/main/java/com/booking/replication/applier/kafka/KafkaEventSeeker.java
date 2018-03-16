package com.booking.replication.applier.kafka;

import com.booking.replication.applier.EventSeeker;
import com.booking.replication.model.*;
import com.booking.replication.model.augmented.AugmentedEventHeader;
import com.booking.replication.model.augmented.AugmentedEventHeaderImplementation;
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
import java.util.*;
import java.util.stream.Collectors;

public class KafkaEventSeeker implements EventSeeker, Comparator<Checkpoint> {
    public interface Configuration extends KafkaEventApplier.Configuration {
        String GROUP_ID = "kafka.group.id";
    }

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final KafkaEventPartitioner partitioner;
    private final Checkpoint[] partitionLast;
    private final BitSet partitionSeeked;
    private boolean seeked;

    public KafkaEventSeeker(KafkaEventPartitioner partitioner, Checkpoint[] partitionLast) {
        this.partitioner = partitioner;
        this.partitionLast = partitionLast;
        this.partitionSeeked = new BitSet(this.partitionLast.length);
        this.seeked = false;
    }

    public KafkaEventSeeker(Map<String, String> configuration, Checkpoint checkpoint) {
        String bootstrapServers = configuration.get(Configuration.BOOTSTRAP_SERVERS);
        String groupId = configuration.get(Configuration.GROUP_ID);
        String topic = configuration.get(Configuration.TOPIC);
        String partitioner = configuration.getOrDefault(Configuration.PARTITIONER, KafkaEventPartitioner.RANDOM.name());

        Objects.requireNonNull(bootstrapServers, String.format("Configuration required: %s", Configuration.BOOTSTRAP_SERVERS));
        Objects.requireNonNull(groupId, String.format("Configuration required: %s", Configuration.GROUP_ID));
        Objects.requireNonNull(topic, String.format("Configuration required: %s", Configuration.TOPIC));

        this.partitioner = KafkaEventPartitioner.valueOf(partitioner);
        this.partitionLast = this.getPartitionLast(checkpoint, bootstrapServers, groupId, topic);
        this.partitionSeeked = new BitSet(this.partitionLast.length);
        this.seeked = false;
    }

    private Checkpoint[] getPartitionLast(Checkpoint checkpoint, String bootstrapServers, String groupId, String topic) {
        Map<String, Object> configuration = new HashMap<>();

        configuration.put("bootstrap.servers", bootstrapServers);
        configuration.put("group.id", groupId);
        configuration.put("key.deserializer", ByteArrayDeserializer.class.getName());
        configuration.put("value.deserializer", ByteArrayDeserializer.class.getName());

        try (Consumer<byte[], byte[]> consumer = new KafkaConsumer<>(configuration)) {
            List<TopicPartition> topicPartitions = consumer.partitionsFor(topic).stream().map(partitionInfo -> new TopicPartition(partitionInfo.topic(), partitionInfo.partition())).collect(Collectors.toList());
            Checkpoint[] partitionLast = new Checkpoint[topicPartitions.stream().mapToInt(TopicPartition::partition).max().orElseThrow(() -> new InvalidPartitionsException("partitions not found")) + 1];

            Arrays.fill(partitionLast, checkpoint);

            for (TopicPartition topicPartition : topicPartitions) {
                consumer.assign(Collections.singletonList(topicPartition));
                Map<TopicPartition, Long> endOffsets = consumer.endOffsets(Collections.singletonList(topicPartition));
                consumer.seek(topicPartition, endOffsets.get(topicPartition));

                ConsumerRecords<byte[], byte[]> consumerRecords = consumer.poll(100);

                for (ConsumerRecord<byte[], byte[]> consumerRecord : consumerRecords) {
                    partitionLast[topicPartition.partition()] = KafkaEventSeeker.MAPPER.readValue(
                            consumerRecord.key(), AugmentedEventHeaderImplementation.class
                    ).getCheckpoint();
                }
            }

            return partitionLast;
        } catch (IOException exception) {
            throw new UncheckedIOException(exception);
        }
    }

    @Override
    public Event apply(Event event) {
        if (this.seeked) {
            return event;
        } else {
            int partition = this.partitioner.partition(event, this.partitionLast.length);

            if (this.partitionLast[partition] == null ||
                this.compare(this.partitionLast[partition], AugmentedEventHeader.class.cast(event.getHeader()).getCheckpoint()) < 0) {
                this.partitionSeeked.set(partition, true);
                this.seeked = this.partitionSeeked.cardinality() == this.partitionSeeked.length();

                return event;
            } else {
                return null;
            }
        }
    }

    @Override
    public int compare(Checkpoint checkpoint1, Checkpoint checkpoint2) {
        if (checkpoint1.getPseudoGTID() != null && checkpoint2.getPseudoGTID() != null) {
            if (checkpoint1.getPseudoGTID().equals(checkpoint2.getPseudoGTID())) {
                return Integer.compare(checkpoint1.getPseudoGTIDIndex(), checkpoint2.getPseudoGTIDIndex());
            } else {
                return checkpoint1.getPseudoGTID().compareTo(checkpoint2.getPseudoGTID());
            }
        } else if (checkpoint1.getPseudoGTID() != null) {
            return Integer.MAX_VALUE;
        } else if (checkpoint2.getPseudoGTID() != null) {
            return Integer.MIN_VALUE;
        } else {
            return Integer.compare(checkpoint1.getPseudoGTIDIndex(), checkpoint2.getPseudoGTIDIndex());
        }
    }
}
