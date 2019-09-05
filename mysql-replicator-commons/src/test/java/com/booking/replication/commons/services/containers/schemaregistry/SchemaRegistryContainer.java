package com.booking.replication.commons.services.containers.schemaregistry;

import com.booking.replication.commons.services.containers.TestContainer;
import com.booking.replication.commons.services.containers.zookeeper.ZookeeperContainer;
import com.github.dockerjava.api.model.PortBinding;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;

import java.time.Duration;

public class SchemaRegistryContainer extends GenericContainer<SchemaRegistryContainer> implements TestContainer<SchemaRegistryContainer> {
    private static final Logger LOG = LogManager.getLogger(SchemaRegistryContainer.class);

    private static final String IMAGE = "confluentinc/cp-schema-registry";
    private static final String TAG = "4.0.1";
    private static final String KAFKASTORE_CONNECTION_URL_KEY = "SCHEMA_REGISTRY_KAFKASTORE_CONNECTION_URL";
    private static final String SCHEMA_REGISTRY_HOST_NAME_KEY = "SCHEMA_REGISTRY_HOST_NAME";
    private static final String SCHEMA_REGISTRY_IMAGE_KEY = "docker.image.schema_registry";
    private static final int SCHEMA_REGISTRY_PORT = 8081;
    private static final String WAIT_REGEX = ".*Server started.*\\n";
    private static final int WAIT_TIMES = 1;

    private SchemaRegistryContainer() {
        this(System.getProperty(SCHEMA_REGISTRY_IMAGE_KEY, String.format("%s:%s", IMAGE, TAG)));
    }

    private SchemaRegistryContainer(final String imageWithTag) {
        super(imageWithTag);

        withExposedPorts(SCHEMA_REGISTRY_PORT);
        waitingFor(Wait.forLogMessage(WAIT_REGEX, WAIT_TIMES).withStartupTimeout(Duration.ofMinutes(5L)));
        withCreateContainerCmdModifier(cmd -> cmd.withPortBindings(PortBinding.parse(String.format("%d:%d", SCHEMA_REGISTRY_PORT, SCHEMA_REGISTRY_PORT))));
        withLogConsumer(outputFrame -> LOG.info(imageWithTag + " " + outputFrame.getUtf8String()));
        withEnv(SCHEMA_REGISTRY_HOST_NAME_KEY, "schema-registry");
    }

    public static SchemaRegistryContainer newInstance() {
        return new SchemaRegistryContainer();
    }

    public static SchemaRegistryContainer newInstance(final String imageWithTag) {
        return new SchemaRegistryContainer(imageWithTag);
    }

    public SchemaRegistryContainer withKafka(final ZookeeperContainer zookeeperContainer) {
        withNetwork(zookeeperContainer.getNetwork());
        if (zookeeperContainer.getNetworkAliases().size() > 1) {
            withEnv(KAFKASTORE_CONNECTION_URL_KEY, String.format("%s:%d", zookeeperContainer.getNetworkAliases().get(1), zookeeperContainer.getPort()));
        } else {
            withEnv(KAFKASTORE_CONNECTION_URL_KEY, String.format("%s:%d", zookeeperContainer.getHost(), zookeeperContainer.getPort()));
        }
        return self();
    }

    @Override
    public SchemaRegistryContainer withNetwork(final Network network) {
        super.withNetwork(network);
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
