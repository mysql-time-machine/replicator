package com.booking.replication.commons.services.containers.zookeeper;

import org.testcontainers.containers.Network;

import java.util.Optional;

public class ZookeeperContainerProvider {

    private ZookeeperContainerProvider() {
    }

    public static ZookeeperContainer start() {
        final ZookeeperContainer zookeeperContainer = ZookeeperContainer.newInstance();
        zookeeperContainer.doStart();
        return zookeeperContainer;
    }

    public static ZookeeperContainer startWithNetwork(final Network network, final String networkAlias) {
        final ZookeeperContainer zookeeperContainer = ZookeeperContainer.newInstance()
            .withNetwork(network);
        Optional.ofNullable(networkAlias).ifPresent(zookeeperContainer::withNetworkAliases);
        zookeeperContainer.doStart();
        return zookeeperContainer;
    }

    public static ZookeeperContainer startWithPortBindings() {
        final ZookeeperContainer zookeeperContainer = ZookeeperContainer.newInstance()
            .withPortBindings();
        zookeeperContainer.doStart();
        return zookeeperContainer;
    }

    public static ZookeeperContainer startWithNetworkAndPortBindings(final Network network) {
        return startWithNetworkAndPortBindings(network, Optional.empty());
    }

    public static ZookeeperContainer startWithNetworkAndPortBindings(final Network network, final String networkAlias) {
        return startWithNetworkAndPortBindings(network, Optional.ofNullable(networkAlias));
    }

    private static ZookeeperContainer startWithNetworkAndPortBindings(final Network network, final Optional<String> networkAlias) {
        final ZookeeperContainer zookeeperContainer = ZookeeperContainer.newInstance()
            .withNetwork(network)
            .withPortBindings();
        networkAlias.ifPresent(zookeeperContainer::withNetworkAliases);
        zookeeperContainer.doStart();
        return zookeeperContainer;
    }
}
