package com.booking.replication.commons.services;

import com.github.dockerjava.api.model.PortBinding;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.FixedHostPortGenericContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;

public final class ContainersProvider implements ServicesProvider {

    private static final String ZOOKEEPER_DOCKER_IMAGE_KEY = "docker.image.zookeeper";
    private static final String ZOOKEEPER_DOCKER_IMAGE_DEFAULT = "zookeeper:latest";
    private static final String ZOOKEEPER_STARTUP_WAIT_REGEX = ".*binding to port.*\\n";
    private static final int ZOOKEEPER_STARTUP_WAIT_TIMES = 1;
    private static final int ZOOKEEPER_PORT = 2181;

    private static final String MYSQL_DOCKER_IMAGE_KEY = "docker.image.mysql";
    private static final String MYSQL_DOCKER_IMAGE_DEFAULT = "mysql:5.6.38";
    private static final String MYSQL_ROOT_PASSWORD_KEY = "MYSQL_ROOT_PASSWORD";
    private static final String MYSQL_DATABASE_KEY = "MYSQL_DATABASE";
    private static final String MYSQL_USER_KEY = "MYSQL_USER";
    private static final String MYSQL_PASSWORD_KEY = "MYSQL_PASSWORD";
    private static final String MYSQL_CONFIGURATION_FILE = "my.cnf";
    private static final String MYSQL_CONFIGURATION_PATH = "/etc/mysql/conf.d/my.cnf";
    private static final String MYSQL_INIT_SCRIPT_PATH = "/docker-entrypoint-initdb.d/%s";
    private static final String MYSQL_STARTUP_WAIT_REGEX = ".*mysqld: ready for connections.*\\n";
    private static final int MYSQL_STARTUP_WAIT_TIMES = 1;
    private static final int MYSQL_PORT = 3306;

    private static final String KAFKA_DOCKER_IMAGE_KEY = "docker.image.kafka";
    private static final String KAFKA_DOCKER_IMAGE_DEFAULT = "wurstmeister/kafka:latest";
    private static final String KAFKA_STARTUP_WAIT_REGEX = ".*starts at Leader Epoch.*\\n";
    private static final String KAFKA_ZOOKEEPER_CONNECT_KEY = "KAFKA_ZOOKEEPER_CONNECT";
    private static final String KAFKA_CREATE_TOPICS_KEY = "KAFKA_CREATE_TOPICS";
    private static final String KAFKA_ADVERTISED_HOST_NAME_KEY = "KAFKA_ADVERTISED_HOST_NAME";
    private static final int KAFKA_PORT = 9092;

    private static final String HBASE_DOCKER_IMAGE_KEY = "docker.image.hbase";
    private static final String HBASE_CREATE_NAMESPACES = "test,schema_history";

    private static final String HBASE_DOCKER_IMAGE_DEFAULT = "harisekhon/hbase-dev:1.3";

    // this name needs to be manyally added to /etc/hosts. For example if testing
    // on localhost then add:
    //
    // 127.0.0.1       HBASE_HOST
    //
    // The reason why we need this is that zookeeper stores the host names for
    // master and region servers and these names need to be /etc/hosts in order
    // to be able to talk to hbase in container. This means either dynamically
    // adding container id/hostname to /etc/hosts when tests are running, or
    // adding the entry once and use a standard name. Since the /etc/hosts requires
    // sudo access, it is simpler to edit it only once.
    private static final String HBASE_HOST_NAME = "HBASE_HOST";
    private static final String HBASE_CONTAINER_NAME = "HBASE_CONTAINER";


    private static final int HBASE_ZK_PORT = 2181;
    private static final int HBASE_MASTER_PORT = 16000;
    private static final int HBASE_REGION_SERVER_PORT = 16201;

    // TODO: implement separate classes for different container types (Kafka, Zookeeper, Hbase, MySQL)
    public ContainersProvider() {
    }

    private GenericContainer<?> getContainer(String image, int port, Network network, String logWaitRegex, int logWaitTimes, boolean matchExposedPort) {

        GenericContainer<?> container = new GenericContainer<>(image)
                .withExposedPorts(port)
                .waitingFor(
                        Wait.forLogMessage(logWaitRegex, logWaitTimes).withStartupTimeout(Duration.ofMinutes(5L))
                );

        if (network != null) {
            container.withNetwork(network);
        }

        if(matchExposedPort) {
            container.withCreateContainerCmdModifier(
                    command -> command.withPortBindings(PortBinding.parse(String.format("%d:%d", port, port)))
            );
        }

        return container;
    }

    private GenericContainer<?> getContainerHBase(String image, Network network, String logWaitRegex, int logWaitTimes, boolean matchExposedPort) {

        FixedHostPortGenericContainer<?> container = new FixedHostPortGenericContainer<>(image);

        container.withNetwork(network);
        container.withNetworkAliases("hbase_alias");

        container.withFixedExposedPort(HBASE_ZK_PORT, HBASE_ZK_PORT);
        container.withFixedExposedPort(16201, HBASE_REGION_SERVER_PORT);
        container.withFixedExposedPort(16000, HBASE_MASTER_PORT);

        container.withCreateContainerCmdModifier(cmd -> cmd.withName(HBASE_CONTAINER_NAME));
        container.withCreateContainerCmdModifier(cmd -> cmd.withHostName(HBASE_HOST_NAME));

        return container;
    }

    private GenericContainer<?> getZookeeper(Network network) {
        return this.getContainer(
                System.getProperty(ContainersProvider.ZOOKEEPER_DOCKER_IMAGE_KEY, ContainersProvider.ZOOKEEPER_DOCKER_IMAGE_DEFAULT),
                ContainersProvider.ZOOKEEPER_PORT,
                network,
                ContainersProvider.ZOOKEEPER_STARTUP_WAIT_REGEX,
                ContainersProvider.ZOOKEEPER_STARTUP_WAIT_TIMES,
                network == null
        );
    }

    public ServicesControl startMySQL(String schema, String username, String password, String... initScripts) {
        GenericContainer<?> mysql = this.getContainer(
                System.getProperty(ContainersProvider.MYSQL_DOCKER_IMAGE_KEY, ContainersProvider.MYSQL_DOCKER_IMAGE_DEFAULT),
                ContainersProvider.MYSQL_PORT,
                null,
                ContainersProvider.MYSQL_STARTUP_WAIT_REGEX,
                ContainersProvider.MYSQL_STARTUP_WAIT_TIMES,
                false
        ).withEnv(ContainersProvider.MYSQL_ROOT_PASSWORD_KEY, password
        ).withEnv(ContainersProvider.MYSQL_DATABASE_KEY, schema
        ).withEnv(ContainersProvider.MYSQL_USER_KEY, username
        ).withEnv(ContainersProvider.MYSQL_PASSWORD_KEY, password
        ).withClasspathResourceMapping(ContainersProvider.MYSQL_CONFIGURATION_FILE, ContainersProvider.MYSQL_CONFIGURATION_PATH, BindMode.READ_ONLY
        );

        for (String initScript : initScripts) {
            mysql.withClasspathResourceMapping(initScript, String.format(ContainersProvider.MYSQL_INIT_SCRIPT_PATH, initScript), BindMode.READ_ONLY);
        }

        mysql.start();

        return new ServicesControl() {
            @Override
            public void close() {
                mysql.stop();
            }

            @Override
            public int getPort() {
                return mysql.getMappedPort(ContainersProvider.MYSQL_PORT);
            }
        };
    }

    public ServicesControl startZookeeper() {
        GenericContainer<?> zookeeper =  this.getZookeeper(null);

        zookeeper.start();

        return new ServicesControl() {
            @Override
            public void close() {
                zookeeper.stop();
            }

            @Override
            public int getPort() {
                return zookeeper.getMappedPort(ContainersProvider.ZOOKEEPER_PORT);
            }
        };
    }

    @Override
    public ServicesControl startZookeeper(Network network) {
        GenericContainer<?> zookeeper =  this.getZookeeper(network);

        zookeeper.start();

        return new ServicesControl() {
            @Override
            public void close() {
                zookeeper.stop();
            }

            @Override
            public int getPort() {
                return zookeeper.getMappedPort(ContainersProvider.ZOOKEEPER_PORT);
            }
        };
    }

    public ServicesControl startKafka(String topic, int partitions, int replicas) {
        Network network = Network.newNetwork();

        GenericContainer<?> zookeeper = this.getZookeeper(network);

        zookeeper.start();

        GenericContainer<?> kafka = this.getContainer(
                System.getProperty(ContainersProvider.KAFKA_DOCKER_IMAGE_KEY, ContainersProvider.KAFKA_DOCKER_IMAGE_DEFAULT),
                ContainersProvider.KAFKA_PORT,
                network,
                ContainersProvider.KAFKA_STARTUP_WAIT_REGEX,
                partitions,
                true
        ).withEnv(
                ContainersProvider.KAFKA_ZOOKEEPER_CONNECT_KEY,
                String.format("%s:%d", zookeeper.getContainerInfo().getConfig().getHostName(), ContainersProvider.ZOOKEEPER_PORT)
        ).withEnv(
                ContainersProvider.KAFKA_CREATE_TOPICS_KEY,
                String.format("%s:%d:%d", topic, partitions, replicas)
        ).withEnv(
                ContainersProvider.KAFKA_ADVERTISED_HOST_NAME_KEY,
                "localhost"
        );

        kafka.start();

        return new ServicesControl() {
            @Override
            public void close() {
                kafka.stop();
                zookeeper.stop();
            }

            @Override
            public int getPort() {
                return kafka.getMappedPort(ContainersProvider.KAFKA_PORT);
            }
        };
    }

    @Override
    public ServicesControl startHbase() {

        Network network = Network.newNetwork();

//        GenericContainer<?> zookeeper = this.getZookeeper(network);

//        zookeeper.start();

        GenericContainer<?> hbase = this.getContainerHBase(
                ContainersProvider.HBASE_DOCKER_IMAGE_DEFAULT,
                network,
                "",
                0,
                true
        );

        hbase.start();

        return new ServicesControl() {

            @Override
            public void close() {
                hbase.stop();
//                zookeeper.stop();
            }

            @Override
            public int getPort() {
                return hbase.getMappedPort(ContainersProvider.HBASE_ZK_PORT);
            }
        };
    }
}
