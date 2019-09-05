package com.booking.replication.commons.services.containers.hbase;

import com.booking.replication.commons.services.containers.zookeeper.ZookeeperContainer;
import org.testcontainers.containers.Network;

public class HBaseContainerProvider {

    private HBaseContainerProvider() {
    }

    public static HBaseContainer startWithNewNetworkAndZookeeper(final ZookeeperContainer zookeeperContainer) {
        final HBaseContainer hbaseContainer = HBaseContainer.newInstance()
            .withNetwork(Network.newNetwork())
            .withZookeeper(zookeeperContainer);
        hbaseContainer.doStart();
        return hbaseContainer;
    }
}
