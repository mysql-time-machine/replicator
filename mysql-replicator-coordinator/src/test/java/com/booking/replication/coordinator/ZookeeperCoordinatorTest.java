package com.booking.replication.coordinator;

import com.booking.replication.commons.checkpoint.Checkpoint;
import com.booking.replication.commons.containers.ContainersControl;
import com.booking.replication.commons.containers.ContainersTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;

public class ZookeeperCoordinatorTest {
    private static ContainersControl containersControl;
    private static AtomicInteger count;
    private static Coordinator coordinator1;
    private static Coordinator coordinator2;

    @BeforeClass
    public static void before() throws Exception {
        ZookeeperCoordinatorTest.containersControl = ContainersTest.startZookeeper();
        ZookeeperCoordinatorTest.count = new AtomicInteger();

        Runnable leadershipTake = () -> {
            ZookeeperCoordinatorTest.count.getAndIncrement();
            try {
                Thread.sleep(500L);
            } catch (InterruptedException exception) {
                throw new RuntimeException(exception);
            }
        };

        Runnable leaderShipLoss = () -> {
            assertEquals(1, ZookeeperCoordinatorTest.count.get());

            ZookeeperCoordinatorTest.count.getAndDecrement();
        };

        Map<String, String> configuration = new HashMap<>();

        configuration.put(Coordinator.Configuration.TYPE, Coordinator.Type.ZOOKEEPER.name());
        configuration.put(ZookeeperCoordinator.Configuration.CONNECTION_STRING, ZookeeperCoordinatorTest.containersControl.getURL());
        configuration.put(ZookeeperCoordinator.Configuration.LEADERSHIP_PATH, "/leadership.coordinator");

        ZookeeperCoordinatorTest.coordinator1 = Coordinator.build(configuration);
        ZookeeperCoordinatorTest.coordinator1.onLeadershipTake(leadershipTake);
        ZookeeperCoordinatorTest.coordinator1.onLeadershipLoss(leaderShipLoss);
        ZookeeperCoordinatorTest.coordinator1.start();

        ZookeeperCoordinatorTest.coordinator2 = Coordinator.build(configuration);
        ZookeeperCoordinatorTest.coordinator2.onLeadershipTake(leadershipTake);
        ZookeeperCoordinatorTest.coordinator2.onLeadershipLoss(leaderShipLoss);
        ZookeeperCoordinatorTest.coordinator2.start();
    }

    @Test
    public void testLeadership() throws InterruptedException {
        Thread.sleep(2000L);
    }

    @Test
    public void testCheckpoint() throws InterruptedException, IOException {
        Thread.sleep(2000L);

        Checkpoint checkpoint1 = new Checkpoint(
                ThreadLocalRandom.current().nextLong(),
                UUID.randomUUID().toString(),
                ThreadLocalRandom.current().nextLong(),
                UUID.randomUUID().toString(),
                ThreadLocalRandom.current().nextInt()
        );

        ZookeeperCoordinatorTest.coordinator1.saveCheckpoint("/checkpoint.coordinator", checkpoint1);

        Checkpoint checkpoint2 = ZookeeperCoordinatorTest.coordinator2.loadCheckpoint("/checkpoint.coordinator");

        assertEquals(checkpoint1, checkpoint2);
    }

    @AfterClass
    public static void after() throws Exception {
        ZookeeperCoordinatorTest.coordinator1.stop();
        ZookeeperCoordinatorTest.coordinator2.stop();
        ZookeeperCoordinatorTest.containersControl.close();

        assertEquals(0, ZookeeperCoordinatorTest.count.get());
    }
}

