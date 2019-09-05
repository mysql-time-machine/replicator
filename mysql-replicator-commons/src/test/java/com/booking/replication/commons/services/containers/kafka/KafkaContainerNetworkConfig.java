package com.booking.replication.commons.services.containers.kafka;

import org.testcontainers.containers.Network;

import java.util.Optional;

public class KafkaContainerNetworkConfig {
    public final Network network;
    public final Optional<String> networkAlias;

    public KafkaContainerNetworkConfig(final Network network) {
        this(network, Optional.empty());
    }

    public KafkaContainerNetworkConfig(final Network network, final String networkAlias) {
        this(network, Optional.ofNullable(networkAlias));
    }

    private KafkaContainerNetworkConfig(final Network network, final Optional<String> networkAlias) {
        this.network = network;
        this.networkAlias = networkAlias;
    }
}
