package com.booking.replication.commons.services.containers.kafka;

import com.booking.replication.commons.services.containers.zookeeper.ZookeeperContainer;

public class KafkaContainerProvider {

    private KafkaContainerProvider() {
    }

    public static KafkaContainer start(final KafkaContainerNetworkConfig networkConfig,
                                       final KafkaContainerTopicConfig topicConfig,
                                       final ZookeeperContainer zookeeperContainer) {
        final KafkaContainer kafkaContainer = KafkaContainer.newInstance()
            .withNetwork(networkConfig.network)
            .withZookeeper(zookeeperContainer)
            .withTopic(topicConfig.topic, topicConfig.partitions, topicConfig.replicas);
        networkConfig.networkAlias.ifPresent(kafkaContainer::withNetworkAlias);
        kafkaContainer.doStart();
        return kafkaContainer;
    }
}
