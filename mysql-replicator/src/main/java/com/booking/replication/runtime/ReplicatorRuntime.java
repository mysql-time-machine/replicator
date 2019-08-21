package com.booking.replication.runtime;

import com.booking.replication.runtime.flink.Flink;
import com.booking.replication.runtime.standalone.StandAlone;

import java.io.IOException;
import java.util.Map;

public interface ReplicatorRuntime {
    enum Type {
        FLINK {
            @Override
            protected ReplicatorRuntime newInstance(Map<String, Object> configuration) throws IOException {
                return new Flink(configuration);
            }
        },
        STANDALONE {
            @Override
            protected ReplicatorRuntime newInstance(Map<String, Object> configuration) {
                return new StandAlone(configuration);
            }
        };

        protected abstract ReplicatorRuntime newInstance(Map<String, Object> configuration) throws IOException;
    }


    interface Configuration {
        String RUNTIME = "replicator.runtime";
    }

    static ReplicatorRuntime build(Map<String, Object> configuration) throws IOException {
        return Type.valueOf(
                configuration.getOrDefault(Configuration.RUNTIME, ReplicatorRuntime.Type.FLINK.name()).toString()
        ).newInstance(configuration);
    }

    void start();
    void stop();
}
