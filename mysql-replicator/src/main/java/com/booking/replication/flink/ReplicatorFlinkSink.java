package com.booking.replication.flink;
import com.booking.replication.augmenter.model.event.AugmentedEvent;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

public class ReplicatorFlinkSink extends FlinkKafkaProducer<AugmentedEvent> {

    public interface Configuration {
        String TOPIC = "kafka.topic";
        String BOOTSTRAP_SERVERS_CONFIG = "bootstrap.servers";
    }

    public ReplicatorFlinkSink(Map<String,Object> configuration) {

       super(

            configuration.get(Configuration.TOPIC).toString(),

            new KeyedSerializationSchema<AugmentedEvent>() {

                private final ObjectMapper MAPPER = new ObjectMapper();

                @Override
                public byte[] serializeKey(AugmentedEvent event) {
                    try {
                        return MAPPER.writeValueAsBytes(event.getHeader());
                    } catch (JsonProcessingException e) {
                        e.printStackTrace();
                    }
                    return null;
                }

                @Override
                public byte[] serializeValue(AugmentedEvent event) {
                    try {
                        return MAPPER.writeValueAsBytes(event.getData());
                    } catch (JsonProcessingException e) {
                        e.printStackTrace();
                    }
                    return null;
                }

                @Override
                public String getTargetTopic(AugmentedEvent event) {
                    return (String) configuration.get(Configuration.TOPIC);
                }
            },

            new HashMap<String, String>() {{
                   put(
                       ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                       configuration.get(Configuration.BOOTSTRAP_SERVERS_CONFIG).toString());
                   }}.entrySet().stream().map(e -> {
                           Properties x = new Properties();
                           x.setProperty(e.getKey(),e.getValue());
                           return x;
                   }).collect(Collectors.toList()).get(0),


            java.util.Optional.of(
                    new FlinkKafkaPartitioner<AugmentedEvent>() {
                        @Override
                        public int partition(AugmentedEvent event, byte[] bytes, byte[] bytes1, String s, int[] ints) {
                            // TODO: call BinlogEventFlinkPartitioner
                            return ThreadLocalRandom.current().nextInt(10);
                        }
                    }
            ),

            Semantic.AT_LEAST_ONCE,

            FlinkKafkaProducer.DEFAULT_KAFKA_PRODUCERS_POOL_SIZE

        );
    }
}
