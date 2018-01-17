package com.booking.replication.coordinator;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class FileCoordinatorTest {
    private AtomicInteger count;
    private Coordinator coordinator1;
    private Coordinator coordinator2;

    @Before
    public void before() throws InterruptedException {
        this.count = new AtomicInteger();

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

        this.coordinator1 = new FileCoordinator(Collections.singletonMap(Coordinator.Configuration.TYPE, Coordinator.Type.FILE.name()));
        this.coordinator1.onLeadershipTake(leadershipTake);
        this.coordinator1.onLeadershipLoss(leaderShipLoss);
        this.coordinator1.start();

        this.coordinator2 = new FileCoordinator(Collections.singletonMap(Coordinator.Configuration.TYPE, Coordinator.Type.FILE.name()));
        this.coordinator2.onLeadershipTake(leadershipTake);
        this.coordinator2.onLeadershipLoss(leaderShipLoss);
        this.coordinator2.start();
    }

    @Test
    public void testLeadership() throws InterruptedException {
        Thread.sleep(2000L);

        assertEquals(1, this.count.get());
    }

    @Test
    public void testCheckpoint() throws InterruptedException, IOException {
        Thread.sleep(2000L);

        byte[] checkpoint1 = new byte[20];

        ThreadLocalRandom.current().nextBytes(checkpoint1);

        coordinator1.storeCheckpoint("/tmp/checkpoint", checkpoint1);

        byte[] checkpoint2 = coordinator2.loadCheckpoint("/tmp/checkpoint");

        assertArrayEquals(checkpoint1, checkpoint2);
    }

    @After
    public void after() throws InterruptedException {
        this.coordinator1.stop();
        this.coordinator2.stop();

        assertEquals(0, this.count.get());
    }
}
