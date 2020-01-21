package com.booking.replication.applier.validation;

import com.booking.replication.applier.Applier;
import com.booking.replication.commons.metrics.Metrics;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by dbatheja on 16/12/2019.
 */
public class ValidationService {

    private static final long VALIDATOR_THROTTLING_DEFAULT = 100;
    private static final long TASK_COUNTER_MAX_RESET = 10000000;

    private static final class ValidationTask {

        private static final Map<String,Object> TARGET_TRANSFORMATION;
        static {
            Map<String,Object> map = new HashMap<>();
            map.put("row_status_column","row_status");
            List<String> ignoreColumns = new ArrayList<>();
            ignoreColumns.add("_replicator_uuid");
            ignoreColumns.add("_replicator_xid");
            ignoreColumns.add("_transaction_uuid");
            ignoreColumns.add("_transaction_xid");
            map.put("ignore_columns", ignoreColumns);
            TARGET_TRANSFORMATION = Collections.unmodifiableMap(map);
        }

        private static final Map<String,Object> SOURCE_TRANSFORMATION;
        static {
            Map<String,Object> map = new HashMap<>();
            map.put("map_null","NULL");
            map.put("convert_timestamps_to_epoch",true);
            SOURCE_TRANSFORMATION = Collections.unmodifiableMap(map);
        }

        @JsonProperty("tag")
        private final String tag;

        @JsonProperty("source")
        private final String source;

        @JsonProperty("target")
        private final String target;

        @JsonProperty("target_transformation")
        private final Map<String,Object> targetTransformation = TARGET_TRANSFORMATION;

        @JsonProperty("source_transformation")
        private final  Map<String,Object> sourceTransformation = SOURCE_TRANSFORMATION;

        private ValidationTask(String tag, String sourceUri, String targetUri) {
            this.tag = tag;
            this.source = sourceUri;
            this.target = targetUri;
        }
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(ValidationService.class);

    public static class Configuration {
        private Configuration() {}
        static final String VALIDATION_BROKER = "validation.broker";
        static final String VALIDATION_TOPIC = "validation.topic";
        static final String VALIDATION_TAG = "validation.tag";
        static final String VALIDATION_THROTTLE_ONE_EVERY = "validation.throttle_one_every";
        public static final String VALIDATION_DATA_SOURCE_NAME = "validation.data_source_name";
        static final String VALIDATION_SOURCE_DOMAIN = "validation.source_domain";
        static final String VALIDATION_TARGET_DOMAIN = "validation.target_domain";
    }

    public static ValidationService getInstance(Map<String, Object> configuration) {

        // Validator is only available for HBase / BigTable
        if (!((String)configuration.getOrDefault(Applier.Configuration.TYPE, "console")).equalsIgnoreCase("hbase")
            || configuration.getOrDefault(Configuration.VALIDATION_BROKER,null) == null) {
            return null;
        }
        if ( configuration.getOrDefault(Configuration.VALIDATION_BROKER,null) == null
             || configuration.getOrDefault(Configuration.VALIDATION_SOURCE_DOMAIN,null) == null
             || configuration.getOrDefault(Configuration.VALIDATION_TARGET_DOMAIN,null) == null
             || configuration.getOrDefault(Configuration.VALIDATION_TOPIC, null) == null
             || configuration.getOrDefault(Configuration.VALIDATION_DATA_SOURCE_NAME, null) == null) {
            throw new IllegalArgumentException("Bad validation configuration");
        }

        Properties properties = new Properties();
        properties.put("bootstrap.servers", configuration.get(Configuration.VALIDATION_BROKER));
        Producer<String,String> producer = new KafkaProducer(properties, new StringSerializer(), new StringSerializer());
        long throttleOneEvery = Long.parseLong(configuration.getOrDefault(Configuration.VALIDATION_THROTTLE_ONE_EVERY, String.valueOf(VALIDATOR_THROTTLING_DEFAULT)).toString());
        Metrics metrics = Metrics.getInstance(configuration);
        return new ValidationService(producer,
                                     (String)configuration.get(Configuration.VALIDATION_TOPIC),
                                     (String)configuration.get(Configuration.VALIDATION_TAG),
                                     throttleOneEvery,
                                     metrics);
    }

    private final double throttleOnePerEvery;
    private final AtomicLong validationTaskCounter = new AtomicLong(0);

    private final Producer<String, String> producer;
    private final String topic;
    private final String tag;
    private final ObjectMapper mapper = new ObjectMapper();
    private final Metrics<?> metrics;

    public ValidationService(Producer<String, String> producer, String topic, String tag, long throttleOnePerEvery, Metrics<?> metrics) {
        this.throttleOnePerEvery = throttleOnePerEvery;
        this.producer = producer;
        this.topic = topic;
        this.tag = tag;
        this.metrics = metrics;
    }

    public void registerValidationTask(String id, String sourceUri, String targetUri) {
        long taskCounter = validationTaskCounter.incrementAndGet();
        if (canSubmitTask(taskCounter)) submitValidationTask(id,sourceUri,targetUri);
        // else drop task
    }

    private synchronized boolean canSubmitTask(long taskCounter) {
        if (taskCounter >= TASK_COUNTER_MAX_RESET) validationTaskCounter.set(taskCounter % TASK_COUNTER_MAX_RESET);
        return taskCounter % throttleOnePerEvery == 0;
    }

    public void submitValidationTask(String id, String sourceUri, String targetUri) {
        try {
            String task = mapper.writeValueAsString( new ValidationTask(tag, sourceUri, targetUri) );
            producer.send(new ProducerRecord<>(topic, id, task ));
            LOGGER.debug("Validation task {} {} submitted", id, task);
            if (metrics != null) metrics.getRegistry().counter("applier.validation.task.submit").inc(1L);
        } catch (JsonProcessingException e) {
            LOGGER.error("Failure serializing validation task {} {} {} {}", id, tag, sourceUri, targetUri, e);
        }
    }

    public long getValidationTaskCounter() {
        return this.validationTaskCounter.get();
    }
}