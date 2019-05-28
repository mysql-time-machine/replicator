package com.booking.replication.coordinator;

import com.booking.replication.commons.checkpoint.Checkpoint;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.EOFException;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

public class ZookeeperCoordinator extends Coordinator {
    private static final Logger LOG = LogManager.getLogger(ZookeeperCoordinator.class);

    public interface Configuration {
        String LEADERSHIP_PATH          = "zookeeper.leadership.path";
        String CONNECTION_STRING        = "zookeeper.connection.string";
        String RETRY_INITIAL_SLEEP      = "zookeeper.retry.initial.sleep";
        String RETRY_MAXIMUM_ATTEMPTS   = "zookeeper.retry.maximum.attempts";
    }

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final CuratorFramework client;
    private final LeaderLatch latch;
    private final AtomicBoolean running;

    public ZookeeperCoordinator(Map<String, Object> configuration) {
        Object leadershipPath       = configuration.get(Configuration.LEADERSHIP_PATH);
        Object connectionString     = configuration.get(Configuration.CONNECTION_STRING);
        Object retryInitialSleep    = configuration.getOrDefault(Configuration.RETRY_INITIAL_SLEEP, "1000");
        Object retryMaximumAttempts = configuration.getOrDefault(Configuration.RETRY_MAXIMUM_ATTEMPTS, "3");

        Objects.requireNonNull(leadershipPath, String.format("Configuration required: %s", Configuration.LEADERSHIP_PATH));
        Objects.requireNonNull(connectionString, String.format("Configuration required: %s", Configuration.CONNECTION_STRING));

        this.client = CuratorFrameworkFactory.newClient(
                String.join(",",this.getList(connectionString)),
                new ExponentialBackoffRetry(
                        Integer.parseInt(retryInitialSleep.toString()),
                        Integer.parseInt(retryMaximumAttempts.toString())
                )
        );

        this.latch      = new LeaderLatch(this.client, leadershipPath.toString());
        this.running    = new AtomicBoolean();

        this.latch.addListener(new LeaderLatchListener() {
            @Override
            public void isLeader() {
                ZookeeperCoordinator.this.takeLeadership();
            }

            @Override
            public void notLeader() {
                ZookeeperCoordinator.this.loseLeadership();
            }
        });
    }

    @SuppressWarnings("unchecked")
    private List<String> getList(Object object) {
        if (List.class.isInstance(object)) {
            return (List<String>) object;
        } else {
            return Collections.singletonList(object.toString());
        }
    }

    @Override
    public void saveCheckpoint(String path, Checkpoint checkpoint) throws IOException {
        try {
            if (checkpoint != null) {
                byte[] bytes = ZookeeperCoordinator.MAPPER.writeValueAsBytes(checkpoint);

                if (this.client.checkExists().forPath(path) != null) {
                    this.client.setData().forPath(path, bytes);
                } else {
                    this.client.create().withMode(CreateMode.PERSISTENT).forPath(path, bytes);
                }
            }
        } catch (Exception exception) {
            throw new IOException(exception);
        }
    }

    @Override
    public Checkpoint loadCheckpoint(String path) throws IOException {
        try {
            if (this.client.checkExists().forPath(path) != null) {
                byte[] bytes = this.client.getData().forPath(path);

                if (bytes != null && bytes.length > 0) {
                    return ZookeeperCoordinator.MAPPER.readValue(bytes, Checkpoint.class);
                } else {
                    return null;
                }
            } else {
                return null;
            }
        } catch (Exception exception) {
            throw new IOException(exception);
        }
    }

    @Override
    public void start() {
        if (!this.running.getAndSet(true)) {
            try {
                this.client.start();
                this.latch.start();
            } catch (Exception exception) {
                throw new RuntimeException(exception);
            }
        }

        super.start();
    }

    @Override
    public void awaitLeadership() {
        try {
            ZookeeperCoordinator.LOG.info("Waiting for leadership.");
            this.latch.await();
        } catch (InterruptedException | EOFException exception) {
            throw new RuntimeException(exception);
        }
    }

    @Override
    public void stop() {
        super.stop();

        if (this.running.getAndSet(false)) {
            try {
                this.latch.close();
                this.client.close();
            } catch (IOException exception) {
                throw new RuntimeException(exception);
            }
        }
    }
}
