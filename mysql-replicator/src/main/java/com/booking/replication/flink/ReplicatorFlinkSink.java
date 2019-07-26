package com.booking.replication.flink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;

public interface ReplicatorFlinkSink extends SinkFunction<Object> {

    Logger LOG = LogManager.getLogger(com.booking.replication.applier.Applier.class);

    boolean forceFlush();

    enum Type {

        CONSOLE {
            @Override
            protected SinkFunction<String> newInstance(Map<String, Object> configuration) {
                return new SinkFunction<String>() {
                    @Override
                    public void invoke(String augmentedEventString) throws Exception {
                        System.out.println("augmentedEvent => " + augmentedEventString.toString());
                    }
                };
            }
        },

        KAFKA {
            @Override
            protected SinkFunction<String> newInstance(Map<String, Object> configuration) {

                FlinkKafkaProducer<String> kafkaProducer = new FlinkKafkaProducer<String>(
                    "localhost:9092",   // broker list
                    "replicator",         // target topic
                    new SimpleStringSchema()     // serialization schema
                );
                kafkaProducer.setWriteTimestampToKafka(true);

                return kafkaProducer;
            }
        },

        HBASE {
            @Override
            protected SinkFunction<String> newInstance(Map<String, Object> configuration)  {
                return null;// new HBaseApplier(configuration);
            }
        },

        COUNT {
            @Override
            protected SinkFunction<String> newInstance(Map<String, Object> configuration) {
                return null;//new CountApplier(configuration);
            }
        };

        protected abstract SinkFunction<String> newInstance(Map<String, Object> configuration);
    }

    interface Configuration {
        String TYPE = "applier.type";
    }

    static SinkFunction<String> build(Map<String, Object> configuration) {
        try {
            return ReplicatorFlinkSink.Type.valueOf(
                    configuration.getOrDefault(
                            ReplicatorFlinkSink.Configuration.TYPE,
                            ReplicatorFlinkSink.Type.HBASE.name()
                    ).toString()
            ).newInstance(configuration);
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }
        return null;
    }
}

