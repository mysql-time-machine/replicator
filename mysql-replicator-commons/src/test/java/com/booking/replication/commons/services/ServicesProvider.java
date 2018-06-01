package com.booking.replication.commons.services;

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

    ServicesControl startMySQL(String schema, String username, String password, String ... commands);

    ServicesControl startKafka(String topic, int partitions, int replicas);

    static ServicesProvider build(Type type) {
        return type.newInstance();
    }
}
