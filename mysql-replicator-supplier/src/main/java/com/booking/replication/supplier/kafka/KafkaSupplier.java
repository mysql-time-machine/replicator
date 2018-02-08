package com.booking.replication.supplier.kafka;

import com.booking.replication.model.*;
import com.booking.replication.supplier.EventSupplier;
import com.booking.replication.supplier.kafka.handler.JSONInvocationHandler;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.reflect.InvocationTargetException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class KafkaSupplier implements EventSupplier {
    public interface Configuration {
        String BOOTSTRAP_SERVERS = "kafka.bootstrap.servers";
        String GROUP_ID = "kafka.group.id";
        String TOPIC = "kafka.topic";
    }

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final Consumer<byte[], byte[]> consumer;
    private final String topic;
    private final List<java.util.function.Consumer<Event>> consumers;
    private final ExecutorService executor;
    private final AtomicBoolean running;

    public KafkaSupplier(Map<String, String> configuration, Checkpoint checkpoint) {
        String bootstrapServers = configuration.get(Configuration.BOOTSTRAP_SERVERS);
        String groupId = configuration.get(Configuration.GROUP_ID);
        String topic = configuration.get(Configuration.TOPIC);

        Objects.requireNonNull(bootstrapServers, String.format("Configuration required: %s", Configuration.BOOTSTRAP_SERVERS));
        Objects.requireNonNull(groupId, String.format("Configuration required: %s", Configuration.GROUP_ID));
        Objects.requireNonNull(topic, String.format("Configuration required: %s", Configuration.TOPIC));

        this.consumer = this.getConsumer(bootstrapServers, groupId);
        this.topic = topic;
        this.consumers = new ArrayList<>();
        this.executor = Executors.newSingleThreadExecutor();
        this.running = new AtomicBoolean();
    }

    private Consumer<byte[],byte[]> getConsumer(String bootstrapServers, String groupId) {
        Map<String, Object> configuration = new HashMap<>();

        configuration.put("bootstrap.servers", bootstrapServers);
        configuration.put("group.id", groupId);
        configuration.put("key.deserializer", ByteArrayDeserializer.class.getName());
        configuration.put("value.deserializer", ByteArrayDeserializer.class.getName());

        return new KafkaConsumer<>(configuration);
    }

    @Override
    public void onEvent(java.util.function.Consumer<Event> consumer) {
        this.consumers.add(consumer);
    }

    @Override
    public void start() {
        this.consumer.subscribe(Collections.singletonList(this.topic));

        this.executor.execute(() -> {
            try {
                this.running.set(true);

                while (this.running.get()) {
                    ConsumerRecords<byte[], byte[]> records = this.consumer.poll(100);

                    for (ConsumerRecord<byte[], byte[]> record : records) {
                        EventHeader header = getHeader(record.key());
                        Event event = new EventImplementation<>(header, this.getData(header, record.value()));

                        this.consumers.forEach(consumer -> consumer.accept(event));
                    }
                }
            } catch (IOException exception) {
                throw new UncheckedIOException(exception);
            } catch (Exception exception) {
                throw new RuntimeException(exception);
            }
        });
    }

    private EventHeader getHeader(byte[] value) throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException, IOException {
        return EventHeader.decorate(new JSONInvocationHandler(KafkaSupplier.MAPPER, value));
    }

    private EventData getData(EventHeader header, byte[] value) throws NoSuchMethodException, IllegalAccessException, java.lang.reflect.InvocationTargetException, InstantiationException, IOException {
        switch (header.getEventType()) {
            case TRANSACTION:
            case AUGMENTED_INSERT:
            case AUGMENTED_UPDATE:
            case AUGMENTED_DELETE:
            case AUGMENTED_SCHEMA:
                return KafkaSupplier.MAPPER.readValue(value, header.getEventType().getImplementation());
            default:
                return EventData.decorate(header.getEventType().getDefinition(), new JSONInvocationHandler(KafkaSupplier.MAPPER, value));
        }
    }

    @Override
    public void stop() {
        this.running.set(false);

        try {
            this.executor.shutdown();
            this.executor.awaitTermination(5L, TimeUnit.SECONDS);
        } catch (InterruptedException exception) {
        } finally {
            this.executor.shutdownNow();
        }

        this.consumer.close();

    }
}
