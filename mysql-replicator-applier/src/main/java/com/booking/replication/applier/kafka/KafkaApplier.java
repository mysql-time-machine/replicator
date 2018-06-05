package com.booking.replication.applier.kafka;

import com.booking.replication.applier.Applier;
import com.booking.replication.augmenter.model.AugmentedEvent;
import com.booking.replication.commons.map.MapFilter;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.errors.InvalidPartitionsException;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.io.UncheckedIOException;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

public class KafkaApplier implements Applier {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public interface Configuration {
        String TOPIC = "kafka.topic";
        String PARTITIONER = "kafka.partitioner";
        String PRODUCER_PREFIX = "kafka.producer.";
    }

    private final Map<String, Producer<byte[], byte[]>> producers;
    private final Map<String, Object> configuration;
    private final String topic;
    private final int totalPartitions;
    private final KafkaPartitioner partitioner;

    public KafkaApplier(Map<String, String> configuration) {
        String topic = configuration.get(Configuration.TOPIC);
        String partitioner = configuration.getOrDefault(Configuration.PARTITIONER, KafkaPartitioner.RANDOM.name());

        Objects.requireNonNull(topic, String.format("Configuration required: %s", Configuration.TOPIC));

        this.producers = new ConcurrentHashMap<>();
        this.configuration = new MapFilter(configuration).filter(Configuration.PRODUCER_PREFIX);
        this.topic = topic;
        this.totalPartitions = this.getTotalPartitions();
        this.partitioner = KafkaPartitioner.valueOf(partitioner);
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
            byte[] header = KafkaApplier.MAPPER.writeValueAsBytes(augmentedEvent.getHeader());
            byte[] data = KafkaApplier.MAPPER.writeValueAsBytes(augmentedEvent.getData());

            this.producers.computeIfAbsent(
                    Thread.currentThread().getName(), key -> this.getProducer()
            ).send(new ProducerRecord<>(
                    this.topic, this.partitioner.partition(augmentedEvent, this.totalPartitions), header, data
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
