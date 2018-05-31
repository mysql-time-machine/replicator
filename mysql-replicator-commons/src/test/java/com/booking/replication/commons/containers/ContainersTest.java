package com.booking.replication.commons.containers;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;

import java.time.Duration;

public final class ContainersTest {
    private ContainersTest() {
    }

    private static GenericContainer getContainer(String image, int port, Network network, String logWaitRegex, int logWaitTimes) {
        GenericContainer container = new GenericContainer(image)
                .withExposedPorts(port)
                .waitingFor(Wait.forLogMessage(logWaitRegex, logWaitTimes).withStartupTimeout(Duration.ofMinutes(5L)));

        if (network != null) {
            container.setNetwork(network);
        }

        return container;
    }

    private static GenericContainer getZookeeper(Network network) {
        return ContainersTest.getContainer(
                System.getProperty("zookeeper.docker.image", "zookeeper:latest"),
                2181,
                network,
                ".*binding to port.*\\n",
                1
        );
    }

    public static ContainersControl startZookeeper() {
        GenericContainer container =  ContainersTest.getZookeeper(null);

        container.start();

        return new ContainersControl() {
            @Override
            public void close() {
                container.stop();
            }

            @Override
            public String getURL() {
                return String.format("%s:%s", container.getContainerIpAddress(), container.getMappedPort(2181));
            }
        };
    }

    public static ContainersControl startKafka(String topic, int partitions, int replicas) {
        Network network = Network.newNetwork();

        GenericContainer zookeeper = ContainersTest.getZookeeper(network);

        zookeeper.start();

        GenericContainer kafka = ContainersTest.getContainer(
                System.getProperty("zookeeper.kafka.image", "wurstmeister/kafka:latest"),
                9092,
                network,
                ".*starts at Leader Epoch.*\\n",
                partitions
        ).withEnv(
                "KAFKA_ZOOKEEPER_CONNECT",
                String.format("%s:2181", zookeeper.getContainerInfo().getConfig().getHostName())
        ).withEnv(
                "KAFKA_CREATE_TOPICS",
                String.format("%s:%d:%d", topic, partitions, replicas)
        );

        kafka.start();

        return new ContainersControl() {
            @Override
            public void close() {
                kafka.stop();
                zookeeper.stop();
            }

            @Override
            public String getURL() {
                return String.format("%s:%s", kafka.getContainerIpAddress(), kafka.getMappedPort(9092));
            }
        };
    }
}
