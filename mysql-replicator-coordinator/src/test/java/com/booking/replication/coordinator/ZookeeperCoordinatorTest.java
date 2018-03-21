package com.booking.replication.coordinator;

import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;

public class ZookeeperCoordinatorTest {
    private AtomicInteger count;
    private Coordinator coordinator1;
    private Coordinator coordinator2;

    @Before
    public void before() throws Exception {
        this.count = new AtomicInteger();

        TestingServer server = new TestingServer();

        Runnable leadershipTake = () -> {
            this.count.getAndIncrement();
            try {
                Thread.sleep(500L);
            } catch (InterruptedException exception) {
                throw new RuntimeException(exception);
            }
        };
        Runnable leaderShipLoss = () -> {
            assertEquals(1, this.count.get());

            this.count.getAndDecrement();
        };

        Map<String, String> configuration = new HashMap<>();

        configuration.put(Coordinator.Configuration.TYPE, Coordinator.Type.ZOOKEEPER.name());
        configuration.put(ZookeeperCoordinator.Configuration.CONNECTION_STRING, server.getConnectString());
        configuration.put(ZookeeperCoordinator.Configuration.LEADERSHIP_PATH, "/leadership.coordinator");

        this.coordinator1 = Coordinator.build(configuration);
        this.coordinator1.onLeadershipTake(leadershipTake);
        this.coordinator1.onLeadershipLoss(leaderShipLoss);
        this.coordinator1.start();

        this.coordinator2 = Coordinator.build(configuration);
        this.coordinator2.onLeadershipTake(leadershipTake);
        this.coordinator2.onLeadershipLoss(leaderShipLoss);
        this.coordinator2.start();
    }

    @Test
    public void testLeadership() throws InterruptedException {
        Thread.sleep(2000L);
    }

    @Test
    public void testCheckpoint() throws InterruptedException, IOException {
        Thread.sleep(2000L);

        String checkpoint1 = UUID.randomUUID().toString();

        coordinator1.storeCheckpoint("/checkpoint.coordinator", checkpoint1);

        String checkpoint2 = coordinator2.loadCheckpoint("/checkpoint.coordinator", String.class);

        assertEquals(checkpoint1, checkpoint2);
    }

    @After
    public void after() throws InterruptedException {
        this.coordinator1.stop();
        this.coordinator2.stop();

        assertEquals(0, this.count.get());
    }
}
