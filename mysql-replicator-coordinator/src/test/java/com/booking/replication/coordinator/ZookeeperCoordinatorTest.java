package com.booking.replication.coordinator;

import com.booking.replication.commons.checkpoint.Binlog;
import com.booking.replication.commons.checkpoint.Checkpoint;
import com.booking.replication.commons.checkpoint.GTID;
import com.booking.replication.commons.checkpoint.GTIDType;
import com.booking.replication.commons.services.ServicesControl;
import com.booking.replication.commons.services.ServicesProvider;
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
    private static ServicesControl servicesControl;
    private static AtomicInteger count;
    private static Coordinator coordinator1;
    private static Coordinator coordinator2;

    @BeforeClass
    public static void before() {
        ZookeeperCoordinatorTest.servicesControl = ServicesProvider.build(ServicesProvider.Type.CONTAINERS).startZookeeper();
        ZookeeperCoordinatorTest.count = new AtomicInteger();

        Runnable leadershipTake = () -> {
            ZookeeperCoordinatorTest.count.getAndIncrement();
        };

        Runnable leaderShipLoss = () -> {
            assertEquals(1, ZookeeperCoordinatorTest.count.get());

            ZookeeperCoordinatorTest.count.getAndDecrement();
        };

        Map<String, Object> configuration = new HashMap<>();

        configuration.put(Coordinator.Configuration.TYPE, Coordinator.Type.ZOOKEEPER.name());
        configuration.put(ZookeeperCoordinator.Configuration.CONNECTION_STRING, ZookeeperCoordinatorTest.servicesControl.getURL());
        configuration.put(ZookeeperCoordinator.Configuration.LEADERSHIP_PATH, "/leadership.coordinator");

        ZookeeperCoordinatorTest.coordinator1 = Coordinator.build(configuration);
        ZookeeperCoordinatorTest.coordinator1.onLeadershipTake(leadershipTake);
        ZookeeperCoordinatorTest.coordinator1.onLeadershipLose(leaderShipLoss);
        ZookeeperCoordinatorTest.coordinator1.start();

        ZookeeperCoordinatorTest.coordinator2 = Coordinator.build(configuration);
        ZookeeperCoordinatorTest.coordinator2.onLeadershipTake(leadershipTake);
        ZookeeperCoordinatorTest.coordinator2.onLeadershipLose(leaderShipLoss);
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
                System.currentTimeMillis(),
                ThreadLocalRandom.current().nextLong(),
                new GTID(
                        GTIDType.PSEUDO,
                        UUID.randomUUID().toString(),
                        Byte.MAX_VALUE
                ),
                new Binlog(
                        UUID.randomUUID().toString(),
                        ThreadLocalRandom.current().nextLong()
                )
        );

        ZookeeperCoordinatorTest.coordinator1.saveCheckpoint("/checkpoint.coordinator", checkpoint1);

        Checkpoint checkpoint2 = ZookeeperCoordinatorTest.coordinator2.loadCheckpoint("/checkpoint.coordinator");

        assertEquals(checkpoint1, checkpoint2);
    }

    @AfterClass
    public static void after() throws Exception {
        ZookeeperCoordinatorTest.coordinator1.stop();
        ZookeeperCoordinatorTest.coordinator2.stop();
        ZookeeperCoordinatorTest.servicesControl.close();

        Thread.sleep(2000L);

        assertEquals(0, ZookeeperCoordinatorTest.count.get());
    }
}

