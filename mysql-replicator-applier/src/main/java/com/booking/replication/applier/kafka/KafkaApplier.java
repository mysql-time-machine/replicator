package com.booking.replication.applier.kafka;

import com.booking.replication.applier.Applier;
import com.booking.replication.applier.Partitioner;
import com.booking.replication.applier.message.format.avro.AvroUtils;
import com.booking.replication.applier.message.format.avro.schema.registry.BCachedSchemaRegistryClient;
import com.booking.replication.applier.validation.ValidationService;
import com.booking.replication.augmenter.model.event.*;
import com.booking.replication.commons.map.MapFilter;
import com.booking.replication.commons.metrics.Metrics;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.fasterxml.jackson.databind.ser.impl.SimpleBeanPropertyFilter;
import com.fasterxml.jackson.databind.ser.impl.SimpleFilterProvider;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.errors.InvalidPartitionsException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

public class KafkaApplier implements Applier {

    private static final Logger LOG = LogManager.getLogger(KafkaApplier.class);

    private final String dataFormat;

    private KafkaAvroSerializer kafkaAvroSerializer;

    private SchemaRegistryClient schemaRegistryClient;

    private final String metricBase;

    private static final ObjectMapper MAPPER = new ObjectMapper();
    {
        Set<String> includeInColumns = new HashSet<>();
        Collections.addAll(includeInColumns, "name", "columnType", "key", "valueDefault", "collation", "nullable");
        SimpleFilterProvider filterProvider = new SimpleFilterProvider();
        filterProvider.addFilter("column", SimpleBeanPropertyFilter.filterOutAllExcept(includeInColumns));
        MAPPER.setFilterProvider(filterProvider);
    }

    public interface Configuration {
        String TOPIC                = "kafka.topic";
        String SCHEMA_REGISTRY_URL  = "kafka.schema.registry.url";
        String PRODUCER_PREFIX      = "kafka.producer.";
        String FORMAT               = "kafka.message.format";
        String INCLUDE_IN_COLUMNS   = "kafka.output.columns.include";
    }

    public interface MessageFormat {
        String AVRO = "avro";
        String JSON = "json";
    }

    private final String METRIC_APPLIER_DELAY;

    private final Map<Integer, Producer<byte[], byte[]>> producers;
    private final Map<String, Object> configuration;

    private final Metrics<?> metrics;
    private final Partitioner partitioner;

    private final String topic;

    private final int totalPartitions;

    private final AtomicReference<AugmentedEvent> lastEventSent = new AtomicReference<>(null);

    public KafkaApplier(Map<String, Object> configuration) {

        Object topic = configuration.get(Configuration.TOPIC);

        Objects.requireNonNull(topic, String.format("Configuration required: %s", Configuration.TOPIC));

        this.producers          = new ConcurrentHashMap<>();
        this.configuration      = new MapFilter(configuration).filter(Configuration.PRODUCER_PREFIX);
        this.topic              = topic.toString();
        this.totalPartitions    = this.getTotalPartitions();
        this.partitioner        = Partitioner.build(configuration);
        this.metrics            = Metrics.getInstance(configuration);
        this.dataFormat         = configuration.get(Configuration.FORMAT) == null ? MessageFormat.AVRO : String.valueOf(configuration.get(Configuration.FORMAT));

        if (Objects.equals(dataFormat, MessageFormat.AVRO)) {
            Object schemaRegistryUrlConfig = configuration.get(Configuration.SCHEMA_REGISTRY_URL);
            Objects.requireNonNull(schemaRegistryUrlConfig, String.format("Configuration required: %s", Configuration.SCHEMA_REGISTRY_URL));
            this.schemaRegistryClient = new BCachedSchemaRegistryClient(String.valueOf(schemaRegistryUrlConfig), 2000);
            //this.kafkaAvroSerializer  = new KafkaAvroSerializer(this.schemaRegistryClient);
        }

        Objects.requireNonNull(topic, String.format("Configuration required: %s", Configuration.TOPIC));

        this.metricBase = MetricRegistry.name(this.metrics.basePath());

        METRIC_APPLIER_DELAY = MetricRegistry.name(
                String.valueOf(configuration.getOrDefault(Metrics.Configuration.BASE_PATH, "")),
                "applier","kafka","delay"
        );

        this.metrics.register(METRIC_APPLIER_DELAY, (Gauge<Long>) () -> {
            if (lastEventSent.get() != null) {
                return System.currentTimeMillis() - lastEventSent.get().getHeader().getTimestamp();
            } else {
                return 0L;
            }
        });

    }

    private Producer<byte[], byte[]> getProducer() {
        LOG.info("Kafka producer configuration : " + configuration.toString());
        return new KafkaProducer<>(this.configuration, new ByteArraySerializer(), new ByteArraySerializer());
    }

    private int getTotalPartitions() {
        try (Producer<byte[], byte[]> producer = this.getProducer()) {
            return producer.partitionsFor(this.topic).stream().mapToInt(PartitionInfo::partition).max().orElseThrow(() -> new InvalidPartitionsException("partitions not found")) + 1;
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
                    for (GenericRecord genericRecord : records) {
                        int schemaId;
                        try {
                            schemaId = this.schemaRegistryClient.register(event.getHeader().schemaKey() + "-value", genericRecord.getSchema());
                            //this.kafkaAvroSerializer.register(event.getHeader().schemaKey() + "-value", genericRecord.getSchema());
                        } catch (RestClientException e) {
                            throw new IllegalStateException("Could not register schema " + new String(event.toJSON()) + "schema: " + genericRecord.getSchema().toString(), e);
                        }
                        byte[] serialized;
                        try {
                            System.out.println("trying to serialize event: " + event.toJSON());
                            serialized = AvroUtils.serializeAvroGenericRecord(genericRecord);
                        } catch (SerializationException e) {
                            throw new IOException("Error serializing data: event header: " +
                                    event.getHeader().toString() +
                                    ", Event json: " +
                                    new String(event.toJSON()), e
                            );
                        }

                        List<Header> headers = new ArrayList<>();
                        headers.add(new RecordHeader("schemaId",  ByteBuffer.allocate(4).putInt(schemaId).array()));

                        ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(
                                this.topic,
                                partition,
                                KafkaApplier.MAPPER.writeValueAsBytes(event.getHeader()),
                                serialized,
                                headers
                        );

                        this.producers.computeIfAbsent(
                                partition, key -> this.getProducer()
                        ).send(record, (metadata, exception) -> {
                            if (exception != null) {
                                // todo: collect producer errors and decide if we have to throw exception here
                                // 1. Exception occurred while writing to kafka:
                                // org.apache.kafka.common.errors.NotLeaderForPartitionException: This server is not the leader for that topic-partition. (A warning. No message is lost)
                                LOG.warn("Exception occurred while writing to kafka: ", exception);
                            }
                        });
                    }

                    this.lastEventSent.set(event);

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

                    byte[] data = KafkaApplier.MAPPER.writeValueAsBytes(event.getData());

                    this.producers.computeIfAbsent(
                            partition, key -> this.getProducer()
                    ).send(new ProducerRecord<>(
                            this.topic,
                            partition,
                            event.getHeader().getTimestamp(),
                            KafkaApplier.MAPPER.writeValueAsBytes(event.getHeader()),
                            data
                    ));

                    this.lastEventSent.set(event);

                    int numRows = getNumberOfRowsInAugmentedEvent(event);

                    writeMetrics(event, numRows);
                }

                return true;
            } catch (JsonProcessingException exception) {
                throw new UncheckedIOException(exception);
            }
        }
    }

    private int getNumberOfRowsInAugmentedEvent(AugmentedEvent event) {
        if (event.getHeader().getEventType().equals(AugmentedEventType.INSERT)) {
            return ((RowsAugmentedEventData) event.getData()).getRows().size();
        }

        if (event.getHeader().getEventType().equals(AugmentedEventType.UPDATE)) {
            return ((UpdateRowsAugmentedEventData) event.getData()).getRows().size();
        }

        if (event.getHeader().getEventType().equals(AugmentedEventType.DELETE)) {
            return ((DeleteRowsAugmentedEventData) event.getData()).getRows().size();
        }
        return 0;
    }

    private void writeMetrics(AugmentedEvent event, int numRows) {
        String tblName = String.valueOf(event.getHeader().getTableName()); //convert null to "null"
        String rowCounter = MetricRegistry.name(this.metricBase, "applier", "kafka", "rows", tblName, event.getHeader().getEventType().name());
        this.metrics.incrementCounter(rowCounter, numRows);
    }

    private void handleIncompatibleSchemaChange(AugmentedEvent event) throws IOException {
        if (event.getData() instanceof QueryAugmentedEventData) {
            List<GenericRecord> genericRecords = event.dataToAvro();

            if (genericRecords.size() != 1) {
                return;
            }

            GenericRecord genericRecord = genericRecords.get(0);
            try {
                Schema schema = new Schema.Parser().parse((String) genericRecord.get("schema"));
                String subject = event.getHeader().schemaKey() + "-value";
                boolean testCompatibility = this.schemaRegistryClient.testCompatibility(subject, schema);

                if (testCompatibility) {
                    return;
                }

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

    // Validation service not ready for Kafka yet
    @Override
    public ValidationService buildValidationService(Map<String, Object> configuration) {
        return null;
    }

    private Set<String> getAsSet(Object object) {
        if (object != null) {
            if (List.class.isInstance(object)) {
                return new HashSet<>((List<String>) object);
            } else {
                return Collections.singleton(object.toString());
            }
        } else {
            return Collections.emptySet();
        }
    }
}
