package com.booking.replication.applier.kafka;

import com.booking.replication.applier.EventApplier;
import com.booking.replication.augmenter.model.AugmentedEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.errors.InvalidPartitionsException;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class KafkaEventApplier implements EventApplier {
    public interface Configuration {
        String TOPIC = "kafka.topic";
        String PARTITIONER = "kafka.partitioner";
        String PRODUCER_PREFIX = "kafka.producer";
    }

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private final Map<String, Producer<byte[], byte[]>> producers;
    private final Map<String, Object> configuration;
    private final String topic;
    private final int totalPartitions;
    private final KafkaEventPartitioner partitioner;

    public KafkaEventApplier(Producer<byte[], byte[]> producer, String topic, int totalPartitions, KafkaEventPartitioner partitioner) {
        this.producers = new ConcurrentHashMap<>(Collections.singletonMap(Thread.currentThread().getName(), producer));
        this.configuration = null;
        this.topic = topic;
        this.totalPartitions = totalPartitions;
        this.partitioner = partitioner;
    }

    public KafkaEventApplier(Map<String, String> configuration) {
        String topic = configuration.get(Configuration.TOPIC);
        String partitioner = configuration.getOrDefault(Configuration.PARTITIONER, KafkaEventPartitioner.RANDOM.name());

        Objects.requireNonNull(topic, String.format("Configuration required: %s", Configuration.TOPIC));

        this.producers = new ConcurrentHashMap<>();
        this.configuration = configuration.entrySet().stream().filter(entry -> entry.getKey().startsWith(Configuration.PRODUCER_PREFIX)).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        this.topic = topic;
        this.totalPartitions = this.getTotalPartitions();
        this.partitioner = KafkaEventPartitioner.valueOf(partitioner);
    }

    private Producer<byte[], byte[]> getProducer() {
        return new KafkaProducer<>(this.configuration, new ByteArraySerializer(), new ByteArraySerializer());
    }

    private int getTotalPartitions() {
        try (Producer<byte[], byte[]> producer = this.getProducer()) {
            return producer.partitionsFor(this.topic).stream().mapToInt(PartitionInfo::partition).max().orElseThrow(() -> new InvalidPartitionsException("partitions not found")) + 1;
        }
    }

    @Override
    public void accept(AugmentedEvent augmentedEvent) {
        try {
            this.producers.computeIfAbsent( // Once per thread
                    Thread.currentThread().getName(),
                    key -> this.getProducer()
            ).send(new ProducerRecord<>(
                    this.topic,
                    this.partitioner.partition(augmentedEvent, this.totalPartitions),
                    KafkaEventApplier.MAPPER.writeValueAsBytes(augmentedEvent.getHeader()),
                    KafkaEventApplier.MAPPER.writeValueAsBytes(augmentedEvent.getData())
            ));
        } catch (JsonProcessingException exception) {
            throw new UncheckedIOException(exception);
        }
    }

    @Override
    public void close() {
        this.producers.values().forEach(Producer::close);
        this.producers.clear();
    }
}
