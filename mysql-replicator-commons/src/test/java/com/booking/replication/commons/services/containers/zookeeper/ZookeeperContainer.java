package com.booking.replication.commons.services.containers.zookeeper;

import com.booking.replication.commons.services.containers.TestContainer;
import com.github.dockerjava.api.model.PortBinding;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;

import java.time.Duration;

public class ZookeeperContainer extends GenericContainer<ZookeeperContainer> implements TestContainer<ZookeeperContainer> {
    private static final Logger LOG = LogManager.getLogger(ZookeeperContainer.class);

    private static final String IMAGE = "zookeeper";
    private static final String TAG = "latest";

    private static final String DOCKER_IMAGE_KEY = "docker.image.zookeeper";
    private static final String WAIT_REGEX = ".*binding to port.*\\n";
    private static final int WAIT_TIMES = 1;
    public static final int PORT = 2181;

    private ZookeeperContainer() {
        this(System.getProperty(DOCKER_IMAGE_KEY, String.format("%s:%s", IMAGE, TAG)));
    }

    private ZookeeperContainer(final String imageWithTag) {
        super(imageWithTag);

        withExposedPorts(PORT);
        waitingFor(Wait.forLogMessage(WAIT_REGEX, WAIT_TIMES).withStartupTimeout(Duration.ofMinutes(5L)));
        withLogConsumer(outputFrame -> LOG.info(imageWithTag + " " + outputFrame.getUtf8String()));
    }

    public static ZookeeperContainer newInstance() {
        return new ZookeeperContainer();
    }

    public static ZookeeperContainer newInstance(final String imageWithTag) {
        return new ZookeeperContainer(imageWithTag);
    }

    @Override
    public ZookeeperContainer withNetwork(final Network network) {
        super.withNetwork(network);
        return self();
    }

    public ZookeeperContainer withPortBindings() {
        withCreateContainerCmdModifier(cmd -> cmd.withPortBindings(PortBinding.parse(String.format("%d:%d", PORT, PORT))));
        return self();
    }

    @Override
    public ZookeeperContainer withNetworkAliases(final String... aliases) {
        super.withNetworkAliases(aliases);
        return self();
    }

    @Override
    public int getPort() {
        return getMappedPort(getContainerInfo().getConfig().getExposedPorts()[0].getPort());
    }

    @Override
    public void doStart() {
        super.doStart();
    }
}
