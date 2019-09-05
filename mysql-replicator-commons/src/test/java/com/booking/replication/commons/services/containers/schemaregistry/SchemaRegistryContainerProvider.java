package com.booking.replication.commons.services.containers.schemaregistry;

import com.booking.replication.commons.services.containers.zookeeper.ZookeeperContainer;

public class SchemaRegistryContainerProvider {

    private SchemaRegistryContainerProvider() {
    }

    public static SchemaRegistryContainer startWithKafka(final ZookeeperContainer kafkaZookeeperContainer) {
        final SchemaRegistryContainer schemaRegistryContainer = SchemaRegistryContainer.newInstance()
            .withKafka(kafkaZookeeperContainer);
        schemaRegistryContainer.doStart();
        return schemaRegistryContainer;
    }

}
