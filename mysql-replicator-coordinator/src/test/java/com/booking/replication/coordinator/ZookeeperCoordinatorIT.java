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

public class ZookeeperCoordinatorIT {
    private static ServicesControl servicesControl;
    private static AtomicInteger count;
    private static Coordinator coordinator1;
    private static Coordinator coordinator2;

    @BeforeClass
    public static void before() {
        ZookeeperCoordinatorIT.servicesControl = ServicesProvider.build(ServicesProvider.Type.CONTAINERS).startZookeeper();
        ZookeeperCoordinatorIT.count = new AtomicInteger();

        Runnable leadershipTake = () -> {
            ZookeeperCoordinatorIT.count.getAndIncrement();
        };

        Runnable leaderShipLoss = () -> {
            assertEquals(1, ZookeeperCoordinatorIT.count.get());

            ZookeeperCoordinatorIT.count.getAndDecrement();
        };

        Map<String, Object> configuration = new HashMap<>();

        configuration.put(Coordinator.Configuration.TYPE, Coordinator.Type.ZOOKEEPER.name());
        configuration.put(ZookeeperCoordinator.Configuration.CONNECTION_STRING, ZookeeperCoordinatorIT.servicesControl.getURL());
        configuration.put(ZookeeperCoordinator.Configuration.LEADERSHIP_PATH, "/leadership.coordinator");

        ZookeeperCoordinatorIT.coordinator1 = Coordinator.build(configuration);
        ZookeeperCoordinatorIT.coordinator1.onLeadershipTake(leadershipTake);
        ZookeeperCoordinatorIT.coordinator1.onLeadershipLose(leaderShipLoss);
        ZookeeperCoordinatorIT.coordinator1.start();

        ZookeeperCoordinatorIT.coordinator2 = Coordinator.build(configuration);
        ZookeeperCoordinatorIT.coordinator2.onLeadershipTake(leadershipTake);
        ZookeeperCoordinatorIT.coordinator2.onLeadershipLose(leaderShipLoss);
        ZookeeperCoordinatorIT.coordinator2.start();
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

        ZookeeperCoordinatorIT.coordinator1.saveCheckpoint("/checkpoint.coordinator", checkpoint1);

        Checkpoint checkpoint2 = ZookeeperCoordinatorIT.coordinator2.loadCheckpoint("/checkpoint.coordinator");

        assertEquals(checkpoint1, checkpoint2);
    }

    @AfterClass
    public static void after() throws Exception {
        ZookeeperCoordinatorIT.coordinator1.stop();
        ZookeeperCoordinatorIT.coordinator2.stop();
        ZookeeperCoordinatorIT.servicesControl.close();

        Thread.sleep(2000L);

        assertEquals(0, ZookeeperCoordinatorIT.count.get());
    }
}

