package com.booking.replication.applier.kafka;

import com.booking.replication.applier.EventApplier;
import com.booking.replication.mysql.binlog.model.Event;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class KafkaEventApplier implements EventApplier {
    interface Configuration {
        String BOOTSTRAP_SERVERS = "kafka.bootstrap.servers";
        String TOPIC = "kafka.topic";
    }

    private final ObjectMapper mapper;
    private final Producer<byte[], byte[]> producer;
    private final String topic;

    public KafkaEventApplier(Map<String, String> configuration) {
        String bootstrapServers = configuration.get(Configuration.BOOTSTRAP_SERVERS);
        String topic = configuration.get(Configuration.TOPIC);

        Objects.requireNonNull(bootstrapServers, String.format("Configuration required: %s", Configuration.BOOTSTRAP_SERVERS));
        Objects.requireNonNull(topic, String.format("Configuration required: %s", Configuration.TOPIC));

        this.mapper = new ObjectMapper();
        this.producer = this.getProducer(bootstrapServers);
        this.topic = topic;
    }

    private Producer<byte[],byte[]> getProducer(String bootstrapServers) {
        Map<String, Object> configuration = new HashMap<>();

        configuration.put("bootstrap.servers", bootstrapServers);
        configuration.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        configuration.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");

        return new KafkaProducer<>(configuration);
    }

    @Override
    public void accept(Event event) {
        try {
            this.producer.send(new ProducerRecord<>(
                    this.topic,
                    this.mapper.writeValueAsBytes(event.getHeader()),
                    this.mapper.writeValueAsBytes(event)
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
