package com.booking.replication.flink;

import com.booking.replication.augmenter.model.event.AugmentedEvent;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collection;
import java.util.Map;

public class ReplicatorFlinkSink extends RichSinkFunction<Object> {

    static Logger LOG = LogManager.getLogger(com.booking.replication.flink.ReplicatorFlinkSink.class);

    enum Type {

        CONSOLE {
            @Override
            protected RichSinkFunction<Collection<AugmentedEvent>> newInstance(Map<String, Object> configuration) {

                return new RichSinkFunction<Collection<AugmentedEvent>>() {
                    @Override
                    public void invoke(Collection<AugmentedEvent> value, Context context) throws Exception {
                        for (AugmentedEvent e: value) {
                            System.out.println("augmentedEvent => " + e.toJSONString());
                        }
                    }
                };
            }
        },

        KAFKA {
            @Override
            protected RichSinkFunction<Collection<AugmentedEvent>> newInstance(Map<String, Object> configuration) {
                return new KafkaSink(configuration);
            }
        },

        HBASE {
            @Override
            protected RichSinkFunction<Collection<AugmentedEvent>> newInstance(Map<String, Object> configuration)  {
                return null;// new HBaseApplier(configuration);
            }
        },

        COUNT {
            @Override
            protected RichSinkFunction<Collection<AugmentedEvent>> newInstance(Map<String, Object> configuration) {
                return null;//new CountApplier(configuration);
            }
        };

        protected abstract RichSinkFunction<Collection<AugmentedEvent>> newInstance(Map<String, Object> configuration);
    }

    interface Configuration {
        String TYPE = "applier.type";
    }

    public static RichSinkFunction<Collection<AugmentedEvent>> build(Map<String, Object> configuration) {
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

