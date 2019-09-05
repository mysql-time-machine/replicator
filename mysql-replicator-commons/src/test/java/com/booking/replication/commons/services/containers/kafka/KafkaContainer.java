package com.booking.replication.commons.services.containers.kafka;

import com.booking.replication.commons.services.containers.TestContainer;
import com.booking.replication.commons.services.containers.zookeeper.ZookeeperContainer;
import com.github.dockerjava.api.model.PortBinding;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;

import java.time.Duration;

public class KafkaContainer extends GenericContainer<KafkaContainer> implements TestContainer<KafkaContainer> {
    private static final Logger LOG = LogManager.getLogger(KafkaContainer.class);

    private static final String IMAGE = "wurstmeister/kafka";
    private static final String TAG = "2.11-1.1.1";

    private static final String KAFKA_DOCKER_IMAGE_KEY = "docker.image.kafka";
    private static final String KAFKA_STARTUP_WAIT_REGEX = ".*starts at Leader Epoch.*\\n";
    private static final int KAFKA_STARTUP_WAIT_TIMES = 1;
    private static final String KAFKA_ZOOKEEPER_CONNECT_KEY = "KAFKA_ZOOKEEPER_CONNECT";
    private static final String KAFKA_CREATE_TOPICS_KEY = "KAFKA_CREATE_TOPICS";
    private static final String KAFKA_LISTENERS_KEY = "KAFKA_LISTENERS";
    private static final String KAFKA_ADVERTISED_LISTENERS_KEY = "KAFKA_ADVERTISED_LISTENERS";
    private static final String KAFKA_LISTENER_SECURITY_PROTOCOL_MAP_KEY = "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP";
    private static final String KAFKA_INTER_BROKER_LISTENER_NAME_KEY = "KAFKA_INTER_BROKER_LISTENER_NAME";
    private static final String KAFKA_ADVERTISED_HOST_NAME_KEY = "KAFKA_ADVERTISED_HOST_NAME";
    private static final int KAFKA_PORT = 9092;

    private KafkaContainer() {
        this(System.getProperty(KAFKA_DOCKER_IMAGE_KEY, String.format("%s:%s", IMAGE, TAG)));
    }

    private KafkaContainer(final String imageWithTag) {
        super(imageWithTag);

        withExposedPorts(KAFKA_PORT);
        waitingFor(Wait.forLogMessage(KAFKA_STARTUP_WAIT_REGEX, KAFKA_STARTUP_WAIT_TIMES).withStartupTimeout(Duration.ofMinutes(5L)));

        withNetwork(Network.newNetwork());
        withCreateContainerCmdModifier(cmd -> cmd.withPortBindings(PortBinding.parse(String.format("%d:%d", KAFKA_PORT, KAFKA_PORT))));
        withLogConsumer(outputFrame -> LOG.info(imageWithTag + " " + outputFrame.getUtf8String()));
    }

    public static KafkaContainer newInstance() {
        return new KafkaContainer();
    }

    public static KafkaContainer newInstance(final String imageWithTag) {
        return new KafkaContainer(imageWithTag);
    }

    public KafkaContainer withTopic(final String topic, final int partitions, final int replicas) {
        withEnv(KAFKA_CREATE_TOPICS_KEY, String.format("%s:%d:%d", topic, partitions, replicas));
        return self();
    }

    @Override
    public KafkaContainer withNetwork(final Network network) {
        super.withNetwork(network);
        return self();
    }

    public KafkaContainer withNetworkAlias(final String networkAlias) {
        super.withNetworkAliases(networkAlias);
        withEnv(KAFKA_LISTENERS_KEY, String.format("PLAINTEXT://%s:29092,OUTSIDE://0.0.0.0:%d", networkAlias, KAFKA_PORT));
        withEnv(KAFKA_LISTENER_SECURITY_PROTOCOL_MAP_KEY, "PLAINTEXT:PLAINTEXT,OUTSIDE:PLAINTEXT");
        withEnv(KAFKA_ADVERTISED_LISTENERS_KEY, String.format("PLAINTEXT://%s:29092,OUTSIDE://localhost:%d", networkAlias, KAFKA_PORT));
        withEnv(KAFKA_INTER_BROKER_LISTENER_NAME_KEY, "PLAINTEXT");// https://rmoff.net/2018/08/02/kafka-listeners-explained/
        return self();
    }

    public KafkaContainer withZookeeper(final ZookeeperContainer zookeeperContainer) {
        if (zookeeperContainer.getNetworkAliases().size() > 1) {
            withEnv(KAFKA_ZOOKEEPER_CONNECT_KEY, String.format("%s:%d", zookeeperContainer.getNetworkAliases().get(1), zookeeperContainer.getPort()));
        } else {
            withEnv(KAFKA_ZOOKEEPER_CONNECT_KEY, String.format("%s:%d", zookeeperContainer.getContainerInfo().getConfig().getHostName(), zookeeperContainer.getPort()));
            withEnv(KAFKA_ADVERTISED_HOST_NAME_KEY, "localhost");
        }
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
