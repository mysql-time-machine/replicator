package com.booking.replication.commons.services.containers.hbase;

import com.booking.replication.commons.services.containers.TestContainer;
import com.booking.replication.commons.services.containers.zookeeper.ZookeeperContainer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testcontainers.containers.FixedHostPortGenericContainer;
import org.testcontainers.containers.Network;

public class HBaseContainer extends FixedHostPortGenericContainer<HBaseContainer> implements TestContainer<HBaseContainer> {
    private static final Logger LOG = LogManager.getLogger(HBaseContainer.class);

    private static final String IMAGE = "harisekhon/hbase-dev";
    private static final String TAG = "1.3";

    // this name needs to be manually added to /etc/hosts. For example if testing
    // on localhost then add:
    //
    // 127.0.0.1       HBASE_HOST
    //
    // The reason why we need this is that zookeeperTag stores the host names for
    // master and region servers and these names need to be /etc/hosts in order
    // to be able to talk to hbase in container. This means either dynamically
    // adding container id/hostname to /etc/hosts when tests are running, or
    // adding the entry once and use a standard name. Since the /etc/hosts requires
    // sudo access, it is simpler to edit it only once.
    private static final String HOST_NAME = "HBASE_HOST";
    private static final String CONTAINER_NAME = "HBASE_CONTAINER";
    private static final int MASTER_PORT = 16000;
    private static final int REGION_SERVER_PORT = 16201;

    private HBaseContainer() {
        this(String.format("%s:%s", IMAGE, TAG));
    }

    private HBaseContainer(final String imageWithTag) {
        super(imageWithTag);

        withNetworkAliases("hbase_alias");
        withFixedExposedPort(16201, REGION_SERVER_PORT);
        withFixedExposedPort(16000, MASTER_PORT);
        withLogConsumer(outputFrame -> LOG.info(imageWithTag + " " + outputFrame.getUtf8String()));
        withCreateContainerCmdModifier(cmd -> cmd.withName(CONTAINER_NAME));
        withCreateContainerCmdModifier(cmd -> cmd.withHostName(HOST_NAME));
    }

    public static HBaseContainer newInstance() {
        return new HBaseContainer();
    }

    public static HBaseContainer newInstance(final String imageWithTag) {
        return new HBaseContainer(imageWithTag);
    }

    @Override
    public HBaseContainer withNetwork(final Network network) {
        super.withNetwork(network);
        return self();
    }

    public HBaseContainer withZookeeper(final ZookeeperContainer zookeeperContainer) {
        if (zookeeperContainer.getExposedPorts().isEmpty()) {
            throw new IllegalStateException("Can't use ZookeeperContainer when there are no exposed ports!");
        }
        final int port = zookeeperContainer.getExposedPorts().get(0);
        withFixedExposedPort(port, port);
        return self();
    }

    @Override
    public int getPort() {
        return getMappedPort(ZookeeperContainer.PORT);
    }

    @Override
    public void doStart() {
        super.doStart();
    }
}
