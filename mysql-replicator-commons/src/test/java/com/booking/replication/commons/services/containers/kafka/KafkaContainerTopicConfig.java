package com.booking.replication.commons.services.containers.kafka;

public class KafkaContainerTopicConfig {
    public final String topic;
    public final int partitions;
    public final int replicas;

    public KafkaContainerTopicConfig(final String topic, final int partitions, final int replicas) {
        this.topic = topic;
        this.partitions = partitions;
        this.replicas = replicas;
    }
}
