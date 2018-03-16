package com.booking.replication.applier.kafka;

import com.booking.replication.applier.EventApplier;
import com.booking.replication.model.Event;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.errors.InvalidPartitionsException;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class KafkaEventApplier implements EventApplier {
    public interface Configuration {
        String BOOTSTRAP_SERVERS = "kafka.bootstrap.servers";
        String TOPIC = "kafka.topic";
        String PARTITIONER = "kafka.partitioner";
    }

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private final Producer<byte[], byte[]> producer;
    private final String topic;
    private final int totalPartitions;
    private final KafkaEventPartitioner partitioner;

    public KafkaEventApplier(Producer<byte[], byte[]> producer, String topic, int totalPartitions, KafkaEventPartitioner partitioner) {
        this.producer = producer;
        this.topic = topic;
        this.totalPartitions = totalPartitions;
        this.partitioner = partitioner;
    }

    public KafkaEventApplier(Map<String, String> configuration) {
        String bootstrapServers = configuration.get(Configuration.BOOTSTRAP_SERVERS);
        String topic = configuration.get(Configuration.TOPIC);
        String partitioner = configuration.getOrDefault(Configuration.PARTITIONER, KafkaEventPartitioner.RANDOM.name());

        Objects.requireNonNull(bootstrapServers, String.format("Configuration required: %s", Configuration.BOOTSTRAP_SERVERS));
        Objects.requireNonNull(topic, String.format("Configuration required: %s", Configuration.TOPIC));

        this.producer = this.getProducer(bootstrapServers);
        this.topic = topic;
        this.totalPartitions = this.getTotalPartitions();
        this.partitioner = KafkaEventPartitioner.valueOf(partitioner);
    }

    private Producer<byte[],byte[]> getProducer(String bootstrapServers) {
        Map<String, Object> configuration = new HashMap<>();

        configuration.put("bootstrap.servers", bootstrapServers);
        configuration.put("key.serializer", ByteArraySerializer.class.getName());
        configuration.put("value.serializer", ByteArraySerializer.class.getName());

        return new KafkaProducer<>(configuration);
    }

    private int getTotalPartitions() {
        return this.producer.partitionsFor(this.topic).stream().mapToInt(PartitionInfo::partition).max().orElseThrow(() -> new InvalidPartitionsException("partitions not found"));
    }

    @Override
    public void accept(Event event) {
        try {
            this.producer.send(new ProducerRecord<>(
                    this.topic,
                    this.partitioner.partition(event, this.totalPartitions),
                    KafkaEventApplier.MAPPER.writeValueAsBytes(event.getHeader()),
                    KafkaEventApplier.MAPPER.writeValueAsBytes(event.getData())
            ));
        } catch (JsonProcessingException exception) {
            throw new UncheckedIOException(exception);
        }
    }

    @Override
    public void close() {
        this.producer.close();
    }
}
