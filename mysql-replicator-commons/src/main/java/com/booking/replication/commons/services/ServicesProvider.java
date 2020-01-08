package com.booking.replication.commons.services;

import com.booking.replication.commons.conf.MySQLConfiguration;
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

    ServicesControl startZookeeper(Network network, String networkAlias);

    ServicesControl startMySQL(MySQLConfiguration mySQLConfiguration);

    ServicesControl startKafka(String topic, int partitions, int replicas);

    ServicesControl startKafka(Network network, String topic, int partitions, int replicas, String networkAlias);

    ServicesControl startSchemaRegistry(Network network);

    ServicesControl startHbase();

    static ServicesProvider build(Type type) {
        return type.newInstance();
    }
}
