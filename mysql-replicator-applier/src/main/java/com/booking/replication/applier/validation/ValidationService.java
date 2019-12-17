package com.booking.replication.applier.validation;

import com.booking.replication.applier.Applier;
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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class ValidationService {

    private static final class ValidationTask {

        private static final Map<String,Object> TARGET_TRANSFORMATION;
        static {
            Map<String,Object> map = new HashMap<>();
            map.put("row_status_column","row_status");
            List<String> ignore_columns = new ArrayList<>();
            ignore_columns.add("_replicator_uuid");
            ignore_columns.add("_replicator_xid");
            ignore_columns.add("_transaction_uuid");
            map.put("ignore_columns", ignore_columns);
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

    public interface Configuration {
        long VALIDATOR_THROTTLING_DEFAULT = TimeUnit.SECONDS.toMillis(5);
        String VALIDATION_BROKER = "validation.broker";
        String VALIDATION_TOPIC = "validation.topic";
        String VALIDATION_TAG = "validation.tag";
        String VALIDATION_THROTTLING = "validation.throttling";
        String VALIDATION_SOURCE_DOMAIN = "validation.source_domain";
        String VALIDATION_TARGET_DOMAIN = "validation.target_domain";
    }

    public static ValidationService getInstance(Map<String, Object> configuration){

        // Validator is only available for HBase / BigTable
        if (!((String)configuration.getOrDefault(Applier.Configuration.TYPE, "console")).toLowerCase().equals("hbase")) {
            return null;
        }

        if ( configuration.getOrDefault(Configuration.VALIDATION_BROKER,null) == null
             || configuration.getOrDefault(Configuration.VALIDATION_SOURCE_DOMAIN,null) == null
             || configuration.getOrDefault(Configuration.VALIDATION_TARGET_DOMAIN,null) == null
             || configuration.getOrDefault(Configuration.VALIDATION_TOPIC, null) == null ) {
            throw new IllegalArgumentException("Bad validation configuration");
        }
        Properties properties = new Properties();
        properties.put("bootstrap.servers", (String)configuration.get(Configuration.VALIDATION_BROKER));
        Producer<String,String> producer = new KafkaProducer(properties, new StringSerializer(), new StringSerializer());

        return new ValidationService(producer,
                                     (String)configuration.get(Configuration.VALIDATION_TOPIC),
                                     (String)configuration.get(Configuration.VALIDATION_TAG),
                                     (long)configuration.getOrDefault(Configuration.VALIDATION_THROTTLING, Configuration.VALIDATOR_THROTTLING_DEFAULT));
    }

    private final long throttlingInterval;
    private long lastRegistrationTime;

    private final AtomicBoolean registrationWeakLock = new AtomicBoolean();

    private final Producer<String, String> producer;
    private final String topic;
    private final String tag;
    private final ObjectMapper mapper = new ObjectMapper();

    public ValidationService(Producer<String, String> producer, String topic,String tag, long throttlingInterval) {

        this.throttlingInterval = throttlingInterval;
        this.producer = producer;
        this.topic = topic;
        this.tag = tag;

    }

    public void registerValidationTask(String id, String sourceUri, String targetUri){

        if (throttlingInterval <= 0 || updateLastRegistrationTime()) submitValidationTask(id,sourceUri,targetUri);

    }

    private boolean updateLastRegistrationTime(){

        long currentTime = System.currentTimeMillis();

        boolean result = false;

        // Double-checked locking WITHOUT volatile:
        // Write to lastRegistrationTime happens-before its second read cause AtomicBoolean write-read sequence is in between.
        // First read may not be consistent (is racy) cause java does not guarantee atomicity for writing longs. But taking,
        // into account the nature of the value it is not a problem
        if (isTimeWindowEmpty(currentTime)){

            if (registrationWeakLock.compareAndSet(false,true)){

                if (isTimeWindowEmpty(currentTime)){

                    lastRegistrationTime = currentTime;

                    result = true;

                }

                registrationWeakLock.set(false);
            }

        }

        return result;
    }

    private boolean isTimeWindowEmpty(long currentTime){

        return currentTime - lastRegistrationTime > throttlingInterval;

    }

    public void submitValidationTask(String id, String sourceUri, String targetUri){

        try {

            String task = mapper.writeValueAsString( new ValidationTask(tag, sourceUri, targetUri) );

            producer.send(new ProducerRecord<>(topic, id, task ));

            LOGGER.info("Validation task {} {} submitted", id, task);

        } catch (JsonProcessingException e) {

            LOGGER.error("Failure serializing validation task {} {} {} {}", id, tag, sourceUri, targetUri, e);

        }

    }


}