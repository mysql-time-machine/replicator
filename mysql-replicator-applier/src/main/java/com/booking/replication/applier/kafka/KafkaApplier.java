package com.booking.replication.applier.kafka;

import com.booking.replication.applier.Applier;
import com.booking.replication.applier.Partitioner;
import com.booking.replication.applier.schema.registry.BCachedSchemaRegistryClient;
import com.booking.replication.augmenter.model.event.AugmentedEvent;
import com.booking.replication.augmenter.model.event.QueryAugmentedEventData;
import com.booking.replication.commons.map.MapFilter;
import com.booking.replication.commons.metrics.Metrics;
import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.errors.InvalidPartitionsException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class KafkaApplier implements Applier {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final String dataFormat;

    private static final Logger LOG = LogManager.getLogger(KafkaApplier.class);

    private final String metricBase;
    private KafkaAvroSerializer kafkaAvroSerializer;
    private SchemaRegistryClient schemaRegistryClient;

    public interface Configuration {
        String TOPIC = "kafka.topic";
        String SCHEMA_REGISTRY_URL = "kafka.schema.registry.url";
        String PRODUCER_PREFIX = "kafka.producer.";

        String BASE_PATH = "events";
        String FORMAT = "kafka.message.format";
    }

    public interface MessageFormat {
        String AVRO = "avro";
        String JSON = "json";
    }

    private final Map<Integer, Producer<byte[], byte[]>> producers;
    private final Map<String, Object> configuration;
    private final String topic;
    private final int totalPartitions;
    private final Partitioner partitioner;
    private final Metrics<?> metrics;
    private final String delayName;

    public KafkaApplier(Map<String, Object> configuration) {
        Object topic = configuration.get(Configuration.TOPIC);

        Objects.requireNonNull(topic, String.format("Configuration required: %s", Configuration.TOPIC));

        this.producers = new ConcurrentHashMap<>();
        this.configuration = new MapFilter(configuration).filter(Configuration.PRODUCER_PREFIX);
        this.topic = topic.toString();
        this.totalPartitions = this.getTotalPartitions();
        this.partitioner = Partitioner.build(configuration);
        this.metrics = Metrics.getInstance(configuration);
        this.dataFormat = configuration.get(Configuration.FORMAT) == null ? MessageFormat.AVRO : String.valueOf(configuration.get(Configuration.FORMAT));

        Object schemaRegistryUrlConfig = configuration.get(Configuration.SCHEMA_REGISTRY_URL);
        if (Objects.equals(dataFormat, MessageFormat.AVRO)) {
            Objects.requireNonNull(topic, String.format("Configuration required: %s", Configuration.SCHEMA_REGISTRY_URL));
            this.schemaRegistryClient = new BCachedSchemaRegistryClient(String.valueOf(schemaRegistryUrlConfig), 2000);
            this.kafkaAvroSerializer = new KafkaAvroSerializer(this.schemaRegistryClient);
        }

        Objects.requireNonNull(topic, String.format("Configuration required: %s", Configuration.TOPIC));

        this.delayName = MetricRegistry.name(
                String.valueOf(configuration.getOrDefault(Metrics.Configuration.BASE_PATH, "events")),
                "delay"
        );
        this.metricBase = MetricRegistry.name(this.metrics.basePath(), "kafka");
    }

    private Producer<byte[], byte[]> getProducer() {
        return new KafkaProducer<>(this.configuration, new ByteArraySerializer(), new ByteArraySerializer());
    }

    private int getTotalPartitions() {
        try (Producer<byte[], byte[]> producer = this.getProducer()) {
            return producer.partitionsFor(this.topic).stream().mapToInt(PartitionInfo::partition).max().orElseThrow(() -> new InvalidPartitionsException("partitions not found")) + 1;
        }
    }

    private List<String> getList(Object object) {
        if (List.class.isInstance(object)) {
            return (List<String>) object;
        } else {
            return Collections.singletonList(object.toString());
        }
    }

    @Override
    public Boolean apply(Collection<AugmentedEvent> events) {
        if (Objects.equals(this.dataFormat, MessageFormat.AVRO)) {

            try {
                for (AugmentedEvent event : events) {
                    handleIncompatibleSchemaChange(event);
                    int partition = this.partitioner.apply(event, this.totalPartitions);
                    List<GenericRecord> records = event.dataToAvro();
                    int numRows = records.size();
                    for (GenericRecord row : records) {
                        //todo: use value.subject.name.strategy
                        try {
                            this.kafkaAvroSerializer.register(event.getHeader().schemaKey() + "-value", row.getSchema());
                        } catch (RestClientException e) {
                            LOG.error("Could not register schema fore event : " + new String(event.toJSON()) + "schema: " + row.getSchema().toString(), e);
                            throw new IllegalStateException("Could not register schema " + new String(event.toJSON()) + "schema: " + row.getSchema().toString(), e);
                        }
                        byte[] serialized;
                        try {
                            serialized = this.kafkaAvroSerializer.serialize(event.getHeader().schemaKey(), row);
                        } catch (SerializationException e) {
                            throw new IOException("Error serializing data: event header: " + event.getHeader().toString()
                                    + ", Event json: " + new String(event.toJSON()), e);
                        }
                        ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(
                                this.topic,
                                partition,
                                KafkaApplier.MAPPER.writeValueAsBytes(event.headerToAvro()),
                                serialized,
                                new RecordHeaders().add(new RecordHeader("meta", event.getHeader().headerString().getBytes()))
                        );

                        this.producers.computeIfAbsent(
                                partition, key -> this.getProducer()
                        ).send(record, (metadata, exception) -> {
                            if (exception != null){
                                // todo: collect producer errors and decide if we have to throw exception here
                                // 1. Exception occurred while writing to kafka:
                                // org.apache.kafka.common.errors.NotLeaderForPartitionException: This server is not the leader for that topic-partition. (A warning. No message is lost)
                                LOG.warn("Exception occurred while writing to kafka: ", exception);
                            }
                        });
                    }

                    writeMetrics(event, numRows);
                }

                return true;
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        } else {
            try {
                for (AugmentedEvent event : events) {
                    int partition = this.partitioner.apply(event, this.totalPartitions);

                    this.producers.computeIfAbsent(
                            partition, key -> this.getProducer()
                    ).send(new ProducerRecord<>(
                            this.topic,
                            partition,
                            event.getHeader().getTimestamp(),
                            KafkaApplier.MAPPER.writeValueAsBytes(event.getHeader()),
                            KafkaApplier.MAPPER.writeValueAsBytes(event.getData())
                    ));

                    writeMetrics(event, 1);
                }

                return true;
            } catch (JsonProcessingException exception) {
                throw new UncheckedIOException(exception);
            }
        }
    }

    private void writeMetrics(AugmentedEvent event, int numRows) {
        this.metrics.updateMeter(this.delayName, System.currentTimeMillis() - event.getHeader().getTimestamp());
        String tblName = String.valueOf(event.getHeader().getTableName()); //convert null to "null"
        String rowCounter = MetricRegistry.name(this.metricBase, tblName, event.getHeader().getEventType().name());
        this.metrics.incrementCounter(rowCounter, numRows);
    }

    private void handleIncompatibleSchemaChange(AugmentedEvent event) throws IOException {
        if (event.getData() instanceof QueryAugmentedEventData) {
            List<GenericRecord> genericRecords = event.dataToAvro();
            if (genericRecords.size() != 1) return;
            GenericRecord genericRecord = genericRecords.get(0);
            try {
                Schema schema = new Schema.Parser().parse((String) genericRecord.get("schema"));
                String subject = event.getHeader().schemaKey() + "-value";
                boolean b = this.schemaRegistryClient.testCompatibility(subject, schema);
                if (b) return;
                QueryAugmentedEventData eventData = (QueryAugmentedEventData) event.getData();
                eventData.setSchemaCompatibilityFlag(false);

                this.schemaRegistryClient.deleteSubject(subject);
            } catch (RestClientException e) {
                LOG.error("Error while checking compatibility of schema", e);
            }
        }
    }

    @Override
    public boolean forceFlush() {
        return false;
    }

    @Override
    public void close() throws IOException {
        this.partitioner.close();
        this.producers.values().forEach(Producer::close);
        this.producers.clear();
    }
}
