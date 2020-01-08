package com.booking.replication.commons.services;

import com.booking.replication.commons.conf.MySQLConfiguration;
import com.github.dockerjava.api.model.PortBinding;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.FixedHostPortGenericContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

public final class ContainersProvider implements ServicesProvider {

    private static final Logger LOG = LogManager.getLogger(ContainersProvider.class);

//    // Tags
//    private static final String MYSQL_DOCKER_IMAGE_DEFAULT = "mysql:5.6.38";
//    private static final String KAFKA_DOCKER_IMAGE_DEFAULT = "wurstmeister/kafka:latest";
//    private static final String ZOOKEEPER_DOCKER_IMAGE_DEFAULT = "zookeeper:latest";
//    private static final String HBASE_DOCKER_IMAGE_DEFAULT = "harisekhon/hbase-dev:1.3";

    private static final String ZOOKEEPER_DOCKER_IMAGE_KEY = "docker.image.zookeeper";
    private static final String ZOOKEEPER_STARTUP_WAIT_REGEX = ".*binding to port.*\\n";
    private static final int ZOOKEEPER_STARTUP_WAIT_TIMES = 1;
    private static final int ZOOKEEPER_PORT = 2181;

    private static final String MYSQL_DOCKER_IMAGE_KEY = "docker.image.mysql";
    private static final String MYSQL_ROOT_PASSWORD_KEY = "MYSQL_ROOT_PASSWORD";
    private static final String MYSQL_DATABASE_KEY = "MYSQL_DATABASE";
    private static final String MYSQL_USER_KEY = "MYSQL_USER";
    private static final String MYSQL_PASSWORD_KEY = "MYSQL_PASSWORD";
    private static final String MYSQL_CONFIGURATION_FILE = "my.cnf";
    private static final String MYSQL_SLAVE_CONFIGURATION_FILE = "myslave.cnf";
    private static final String MYSQL_CONFIGURATION_PATH = "/etc/mysql/conf.d/my.cnf";
    private static final String MYSQL_INIT_SCRIPT_PATH = "/docker-entrypoint-initdb.d/%s";
    private static final String MYSQL_STARTUP_WAIT_REGEX = ".*mysqld: ready for connections.*\\n";
    private static final int MYSQL_STARTUP_WAIT_TIMES = 1;
    private static final int MYSQL_PORT = 3306;

    private static final String KAFKA_DOCKER_IMAGE_KEY = "docker.image.kafka";
    private static final String KAFKA_STARTUP_WAIT_REGEX = ".*starts at Leader Epoch.*\\n";
    private static final String KAFKA_ZOOKEEPER_CONNECT_KEY = "KAFKA_ZOOKEEPER_CONNECT";
    private static final String KAFKA_CREATE_TOPICS_KEY = "KAFKA_CREATE_TOPICS";
    private static final String KAFKA_ADVERTISED_HOST_NAME_KEY = "KAFKA_ADVERTISED_HOST_NAME";

    private static final String KAFKA_LISTENERS_KEY = "KAFKA_LISTENERS";
    private static final String KAFKA_ADVERTISED_LISTENERS_KEY = "KAFKA_ADVERTISED_LISTENERS";
    private static final String KAFKA_LISTENER_SECURITY_PROTOCOL_MAP_KEY = "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP";
    private static final String KAFKA_INTER_BROKER_LISTENER_NAME_KEY = "KAFKA_INTER_BROKER_LISTENER_NAME";
    private static final int KAFKA_PORT = 9092;

    private static final String SCHEMA_REGISTRY_IMAGE_KEY = "docker.image.schema_registry";
    private static final int SCHEMA_REGISTRY_PORT = 8081;
    private static final String SCHEMA_REGISTRY_WAIT_REGEX = ".*Server started.*\\n";
    public static final String SCHEMA_REGISTRY_KAFKASTORE_CONNECTION_URL_KEY = "SCHEMA_REGISTRY_KAFKASTORE_CONNECTION_URL";
    public static final String SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS_KEY = "SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS";
    public static final String SCHEMA_REGISTRY_HOST_NAME_KEY = "SCHEMA_REGISTRY_HOST_NAME";
    public static final String SCHEMA_REGISTRY_LISTENERS_KEY = "SCHEMA_REGISTRY_LISTENERS";
    public static final String KAFKASTORE_TOPIC_KEY = "KAFKASTORE_TOPIC";

    private static final String HBASE_DOCKER_IMAGE_KEY = "docker.image.hbase";
    private static final String HBASE_CREATE_NAMESPACES = "test,schema_history";

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

        if (matchExposedPort) {
            container.withCreateContainerCmdModifier(
                    command -> command.withPortBindings(PortBinding.parse(String.format("%d:%d", port, port)))
            );
        }

        container.withLogConsumer(outputFrame -> {
//            System.out.println(image + " " + outputFrame.getUtf8String());
        });


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

    private GenericContainer<?> getZookeeper(Network network, String zkImageTag) {

        return this.getContainer(
                System.getProperty(
                        ContainersProvider.ZOOKEEPER_DOCKER_IMAGE_KEY,
                        zkImageTag
                ),
                ContainersProvider.ZOOKEEPER_PORT,
                network,
                ContainersProvider.ZOOKEEPER_STARTUP_WAIT_REGEX,
                ContainersProvider.ZOOKEEPER_STARTUP_WAIT_TIMES,
                network == null
        );
    }

    public ServicesControl startCustomTagMySQL(
            String mysqlImageTag,
            String schema,
            String username,
            String password,
            String... initScripts) {
        GenericContainer<?> mysql = this.getContainer(
                System.getProperty(
                        ContainersProvider.MYSQL_DOCKER_IMAGE_KEY,
                        mysqlImageTag
                ),
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
            public GenericContainer<?> getContainer() { return mysql; }

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

    @Override
    public ServicesControl startZookeeper() {
        GenericContainer<?> zookeeper = this.getZookeeper(
                null, VersionedPipelines.defaultTags.zookeeperTag);

        zookeeper.start();


        return new ServicesControl() {
            @Override
            public GenericContainer<?> getContainer() { return zookeeper; }

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
    public ServicesControl startZookeeper(Network network, String networkAlias) {
        GenericContainer<?> zookeeper = this.getZookeeper(network, VersionedPipelines.defaultTags.zookeeperTag);
        zookeeper.withNetworkAliases(networkAlias);

        zookeeper.start();
        return new ServicesControl() {
            @Override
            public GenericContainer<?> getContainer() { return zookeeper; }

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
    public ServicesControl startMySQL(MySQLConfiguration mySQLConfiguration) {

        Map<String, String> envConfigs = new HashMap<>();
        // Root password is mandatory for starting mysql docker instance
        envConfigs.put(ContainersProvider.MYSQL_ROOT_PASSWORD_KEY, mySQLConfiguration.getPassword());
        envConfigs.computeIfAbsent(ContainersProvider.MYSQL_DATABASE_KEY, val -> mySQLConfiguration.getSchema());
        envConfigs.computeIfAbsent(ContainersProvider.MYSQL_USER_KEY, val -> mySQLConfiguration.getUsername());
        envConfigs.computeIfAbsent(ContainersProvider.MYSQL_PASSWORD_KEY, val -> mySQLConfiguration.getPassword());

        GenericContainer<?> mysql = this.getContainer(
                System.getProperty(
                        ContainersProvider.MYSQL_DOCKER_IMAGE_KEY,
                        VersionedPipelines.defaultTags.mysqlReplicantTag
                ),
                ContainersProvider.MYSQL_PORT,
                mySQLConfiguration.getNetwork(),
                ContainersProvider.MYSQL_STARTUP_WAIT_REGEX,
                ContainersProvider.MYSQL_STARTUP_WAIT_TIMES,
                // Cannot match exposed port in mysql as it can have conflicts
                false)
                .withEnv(envConfigs)
                .withClasspathResourceMapping(mySQLConfiguration.getConfPath(), ContainersProvider.MYSQL_CONFIGURATION_PATH, BindMode.READ_ONLY
        );

        for (String initScript : mySQLConfiguration.getInitScripts()) {
            mysql.withClasspathResourceMapping(initScript, String.format(ContainersProvider.MYSQL_INIT_SCRIPT_PATH, initScript), BindMode.READ_ONLY);
        }

        mysql.withNetworkAliases(mySQLConfiguration.getNetworkAlias());

        mysql.start();

        return new ServicesControl() {
            @Override
            public GenericContainer<?> getContainer() { return mysql; }

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

    @Override
    public ServicesControl startKafka(String topic, int partitions, int replicas) {
        Network network = Network.newNetwork();

        GenericContainer<?> zookeeper = this.getZookeeper(network, VersionedPipelines.defaultTags.zookeeperTag);

        zookeeper.start();

        GenericContainer<?> kafka = this.getContainer(
                System.getProperty(
                        ContainersProvider.KAFKA_DOCKER_IMAGE_KEY,
                        VersionedPipelines.defaultTags.kafkaTag
                ),
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
            public GenericContainer<?> getContainer() { return kafka; }

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
    public ServicesControl startKafka(Network network, String topic, int partitions, int replicas, String networkAlias) {

        GenericContainer<?> kafka = this.getContainer(
                System.getProperty(
                        ContainersProvider.KAFKA_DOCKER_IMAGE_KEY,
                        VersionedPipelines.defaultTags.kafkaTag
                ),
                ContainersProvider.KAFKA_PORT,
                network,
                ContainersProvider.KAFKA_STARTUP_WAIT_REGEX,
                partitions,
                true
        ).withEnv(
                ContainersProvider.KAFKA_ZOOKEEPER_CONNECT_KEY,
                String.format("%s:%d", "kafkaZk", ContainersProvider.ZOOKEEPER_PORT)
        ).withEnv(
                ContainersProvider.KAFKA_CREATE_TOPICS_KEY,
                String.format("%s:%d:%d", topic, partitions, replicas)
        ).withEnv(KAFKA_LISTENERS_KEY, String.format("PLAINTEXT://%s:29092,OUTSIDE://0.0.0.0:%d", networkAlias, KAFKA_PORT)
        ).withEnv(KAFKA_LISTENER_SECURITY_PROTOCOL_MAP_KEY, "PLAINTEXT:PLAINTEXT,OUTSIDE:PLAINTEXT"
        ).withEnv(KAFKA_ADVERTISED_LISTENERS_KEY, String.format("PLAINTEXT://%s:29092,OUTSIDE://localhost:%d", networkAlias, KAFKA_PORT)
        ).withEnv(KAFKA_INTER_BROKER_LISTENER_NAME_KEY, "PLAINTEXT");
        //   https://rmoff.net/2018/08/02/kafka-listeners-explained/

        kafka.withNetworkAliases(networkAlias);

        kafka.start();

        return new ServicesControl() {
            @Override
            public GenericContainer<?> getContainer() { return kafka; }

            @Override
            public void close() {
                kafka.stop();
            }

            @Override
            public int getPort() {
                return kafka.getMappedPort(ContainersProvider.KAFKA_PORT);
            }
        };
    }


    public ServicesControl startKafka(String kafkaImageTag, String topic, int partitions, int replicas) {
        Network network = Network.newNetwork();

        GenericContainer<?> zookeeper = this.getZookeeper(
                network,
                VersionedPipelines.defaultTags.zookeeperTag
        );

        zookeeper.start();

        GenericContainer<?> kafka = this.getContainer(
                System.getProperty(
                        ContainersProvider.KAFKA_DOCKER_IMAGE_KEY,
                        kafkaImageTag
                ),
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
            public GenericContainer<?> getContainer() { return kafka; }

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
    public ServicesControl startSchemaRegistry(Network network) {

        GenericContainer<?> schemaRegistry = this.getContainer(System.getProperty(
                ContainersProvider.SCHEMA_REGISTRY_IMAGE_KEY,
                VersionedPipelines.defaultTags.schemaRegistryTag
                ),
                ContainersProvider.SCHEMA_REGISTRY_PORT,
                network,
                ContainersProvider.SCHEMA_REGISTRY_WAIT_REGEX,
                1,
                true
        ).withEnv(
                ContainersProvider.SCHEMA_REGISTRY_KAFKASTORE_CONNECTION_URL_KEY,
                String.format("%s:%d", "kafkaZk", ContainersProvider.ZOOKEEPER_PORT)
        ).withEnv(
                ContainersProvider.SCHEMA_REGISTRY_HOST_NAME_KEY,
                "localhost"
        );

        schemaRegistry.start();

        return new ServicesControl() {
            @Override
            public GenericContainer<?> getContainer() { return schemaRegistry; }

            @Override
            public void close() {
                schemaRegistry.stop();
            }

            @Override
            public int getPort() {
                return schemaRegistry.getMappedPort(ContainersProvider.SCHEMA_REGISTRY_PORT);
            }
        };
    }

    @Override
    public ServicesControl startHbase() {

        Network network = Network.newNetwork();

        GenericContainer<?> hbase = this.getContainerHBase(
                VersionedPipelines.defaultTags.hbase,
                network,
                "",
                0,
                true
        );

        hbase.start();

        return new ServicesControl() {
            @Override
            public GenericContainer<?> getContainer() { return hbase; }

            @Override
            public void close() {
                hbase.stop();
            }

            @Override
            public int getPort() {
                return hbase.getMappedPort(ContainersProvider.HBASE_ZK_PORT);
            }
        };
    }
}
