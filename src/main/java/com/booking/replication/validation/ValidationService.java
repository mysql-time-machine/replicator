package com.booking.replication.validation;

import com.booking.replication.Configuration;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by psalimov on 10/24/16.
 */
public class ValidationService {

    private static final class ValidationTask{

        private final String tag;

        private final String sourceUri;

        private final String targetUri;

        private ValidationTask(String tag, String sourceUri, String targetUri) {
            this.tag = tag;
            this.sourceUri = sourceUri;
            this.targetUri = targetUri;
        }

        public String getTag() {
            return tag;
        }

        public String getSourceUri() {
            return sourceUri;
        }

        public String getTargetUri() {
            return targetUri;
        }
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(ValidationService.class);

    public static ValidationService getInstance(Configuration configuration){

        Configuration.ValidationConfiguration validationConfig = configuration.getValidationConfiguration();

        if (validationConfig == null) return null;

        if (validationConfig.getBroker() == null
                || validationConfig.getSourceDomain() == null
                || validationConfig.getTargetDomain() == null
                || validationConfig.getTopic() == null) throw new IllegalArgumentException("Bad validation configuration");

        Properties properties = new Properties();
        properties.put("bootstrap.servers", validationConfig.getBroker());

        Producer<String,String> producer = new KafkaProducer(properties, new StringSerializer(), new StringSerializer());


        return new ValidationService(producer, validationConfig.getTopic(), validationConfig.getTag(), validationConfig.getThrottling());

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

        // Double-checked locking WITHOUT volatile:
        // Write to lastRegistrationTime happens-before its second read cause AtomicBoolean write-read sequence is in between.
        // First read may not be consistent (is racy) cause java does not guarantee atomicity for writing longs. But taking,
        // into account the nature of the value it is not a problem
        if (isTimeWindowEmpty(currentTime)){

            if (registrationWeakLock.compareAndSet(false,true)){

                if (isTimeWindowEmpty(currentTime)){

                    lastRegistrationTime = currentTime;

                    registrationWeakLock.set(false);

                    return true;

                }
            }

        }

        return false;
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
