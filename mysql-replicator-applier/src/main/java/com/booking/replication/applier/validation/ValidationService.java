package com.booking.replication.applier.validation;

import com.booking.replication.applier.Applier;
import com.booking.replication.commons.metrics.Metrics;
import com.booking.validator.data.source.DataSource;
import com.booking.validator.task.Task;
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

    private static final Logger LOGGER = LoggerFactory.getLogger(ValidationService.class);

    public static class Configuration {
        public static final String VALIDATION_BROKER = "validation.broker";
        public static final String VALIDATION_TOPIC = "validation.topic";
        public static final String VALIDATION_TAG = "validation.tag";
        public static final String VALIDATION_THROTTLE_ONE_EVERY = "validation.throttle_one_every";
        public static final String VALIDATION_SOURCE_DATA_SOURCE = "validation.source_data_source";
        public static final String VALIDATION_TARGET_DATA_SOURCE = "validation.target_data_source";
    }

    public static ValidationService getInstance(Map<String, Object> configuration) {

        // Validator is only available for HBase / BigTable
        if (!((String)configuration.getOrDefault(Applier.Configuration.TYPE, "console")).equalsIgnoreCase("hbase")
            || configuration.getOrDefault(Configuration.VALIDATION_BROKER,null) == null) {
            return null;
        }
        if ( configuration.getOrDefault(Configuration.VALIDATION_BROKER,null) == null
             || configuration.getOrDefault(Configuration.VALIDATION_SOURCE_DATA_SOURCE,null) == null
             || configuration.getOrDefault(Configuration.VALIDATION_TARGET_DATA_SOURCE,null) == null
             || configuration.getOrDefault(Configuration.VALIDATION_TOPIC, null) == null) {
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
    private final Metrics<?> metrics;

    public ValidationService(Producer<String, String> producer, String topic, String tag, long throttleOnePerEvery, Metrics<?> metrics) {
        this.throttleOnePerEvery = throttleOnePerEvery;
        this.producer = producer;
        this.topic = topic;
        this.tag = tag;
        this.metrics = metrics;
    }

    public void registerValidationTask(String id, DataSource source, DataSource target) {
        long taskCounter = validationTaskCounter.incrementAndGet();
        if (canSubmitTask(taskCounter)) submitValidationTask(id,source,target);
        // else drop task
    }

    private synchronized boolean canSubmitTask(long taskCounter) {
        if (taskCounter >= TASK_COUNTER_MAX_RESET) validationTaskCounter.set(taskCounter % TASK_COUNTER_MAX_RESET);
        return taskCounter % throttleOnePerEvery == 0;
    }

    public void submitValidationTask(String id, DataSource source, DataSource target) {
        String task = new Task(tag, source, target, null).toJson();
        if (task.isEmpty()) return;
        producer.send(new ProducerRecord<>(topic, id,  task ));
        LOGGER.debug("Validation task {} {} submitted", id, task);
        if (metrics != null) metrics.getRegistry().counter("applier.validation.task.submit").inc(1L);
    }

    public long getValidationTaskCounter() {
        return this.validationTaskCounter.get();
    }
}