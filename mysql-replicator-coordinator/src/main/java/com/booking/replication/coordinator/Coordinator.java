package com.booking.replication.coordinator;

import java.util.Map;
import java.util.concurrent.TimeUnit;

public interface Coordinator extends LeaderCoordinator, CheckpointCoordinator {
    enum Type {
        ZOOKEEPER {
            @Override
            public Coordinator newInstance(Map<String, String> configuration) {
                return new ZookeeperCoordinator(configuration);
            }
        },
        FILE {
            @Override
            public Coordinator newInstance(Map<String, String> configuration) {
                return new FileCoordinator(configuration);
            }
        };

        public abstract Coordinator newInstance(Map<String, String> configuration);
    }

    interface Configuration {
        String TYPE = "coordinator.type";
    }

    void start() throws InterruptedException;

    void wait(long timeout, TimeUnit unit) throws InterruptedException;

    void join() throws InterruptedException;

    void stop() throws InterruptedException;

    static Coordinator build(Map<String, String> configuration) {
        return Type.valueOf(
                configuration.getOrDefault(Configuration.TYPE, Type.FILE.name())
        ).newInstance(configuration);
    }
}
