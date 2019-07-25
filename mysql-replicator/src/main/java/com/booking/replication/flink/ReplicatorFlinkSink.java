package com.booking.replication.flink;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;

public interface ReplicatorFlinkSink extends SinkFunction<Object> {

    Logger LOG = LogManager.getLogger(com.booking.replication.applier.Applier.class);

    boolean forceFlush();

    enum Type {

        CONSOLE {
            @Override
            protected SinkFunction<Object> newInstance(Map<String, Object> configuration) {
                return new SinkFunction<Object>() {
                    @Override
                    public void invoke(Object augmentedEventString) throws Exception {
                        System.out.println("augmentedEvent => " + augmentedEventString.toString());
                    }
                };
            }
        },

        HBASE {
            @Override
            protected SinkFunction<Object> newInstance(Map<String, Object> configuration)  {
                return null;// new HBaseApplier(configuration);
            }
        },
        KAFKA {
            @Override
            protected SinkFunction<Object> newInstance(Map<String, Object> configuration) {
                return null;//new KafkaApplier(configuration);
            }
        },
        COUNT {
            @Override
            protected SinkFunction<Object> newInstance(Map<String, Object> configuration) {
                return null;//new CountApplier(configuration);
            }
        };

        protected abstract SinkFunction<Object> newInstance(Map<String, Object> configuration);
    }

    interface Configuration {
        String TYPE = "applier.type";
    }

    static SinkFunction<Object> build(Map<String, Object> configuration) {
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

