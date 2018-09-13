package com.booking.replication.commons.services;

import org.testcontainers.containers.Network;

public interface ServicesProvider {

    enum Type {
        CONTAINERS {
            @Override
            public ServicesProvider newInstance() {
                return new ContainersProvider();
            }
        },
        LOCAL {
            @Override
            public ServicesProvider newInstance() {
                return null;
            }
        };

        public abstract ServicesProvider newInstance();

    }

    ServicesControl startZookeeper();

    ServicesControl startZookeeper(Network network);

    ServicesControl startMySQL(String schema, String username, String password, String... initScripts);

    ServicesControl startKafka(String topic, int partitions, int replicas);

    ServicesControl startHbase();

    static ServicesProvider build(Type type) {
        return type.newInstance();
    }
}
