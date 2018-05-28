package com.booking.replication.coordinator;

import com.booking.replication.supplier.model.checkpoint.Checkpoint;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListener;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

public class ZookeeperCoordinator extends Coordinator implements LeaderSelectorListener {
    public interface Configuration {
        String LEADERSHIP_PATH = "zookeeper.leadership.path";
        String CONNECTION_STRING = "zookeeper.connection.string";
        String RETRY_INITIAL_SLEEP = "zookeeper.retry.initial.sleep";
        String RETRY_MAXIMUM_ATTEMPTS = "zookeeper.retry.maximum.attempts";
    }

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final long WAIT_STEP_MILLIS = 100;

    private final CuratorFramework client;
    private final LeaderSelector selector;

    public ZookeeperCoordinator(Map<String, String> configuration) {
        String leadershipPath = configuration.get(Configuration.LEADERSHIP_PATH);
        String connectionString = configuration.get(Configuration.CONNECTION_STRING);
        String retryInitialSleep = configuration.getOrDefault(Configuration.RETRY_INITIAL_SLEEP, "1000");
        String retryMaximumAttempts = configuration.getOrDefault(Configuration.RETRY_MAXIMUM_ATTEMPTS, "3");

        Objects.requireNonNull(leadershipPath, String.format("Configuration required: %s", Configuration.LEADERSHIP_PATH));
        Objects.requireNonNull(connectionString, String.format("Configuration required: %s", Configuration.CONNECTION_STRING));

        this.client = CuratorFrameworkFactory.newClient(connectionString, new ExponentialBackoffRetry(Integer.parseInt(retryInitialSleep), Integer.parseInt(retryMaximumAttempts)));
        this.selector = new LeaderSelector(this.client, leadershipPath, this);

        this.client.start();
    }

    @Override
    public void storeCheckpoint(String path, Checkpoint checkpoint) throws IOException {
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
    public void takeLeadership(CuratorFramework client) {
        this.takeLeadership();
    }

    @Override
    public void stateChanged(CuratorFramework client, ConnectionState newState) {
        if (client.getConnectionStateErrorPolicy().isErrorState(newState)) {
            this.lossLeadership();
        }
    }

    @Override
    public void start() {
        this.selector.start();
    }

    @Override
    public void wait(long timeout, TimeUnit unit) throws InterruptedException {
        long remainMillis = unit.toMillis(timeout);

        while (remainMillis > 0 && this.client.getState() != CuratorFrameworkState.STOPPED) {
            long sleepMillis = remainMillis > ZookeeperCoordinator.WAIT_STEP_MILLIS ? ZookeeperCoordinator.WAIT_STEP_MILLIS : remainMillis;
            Thread.sleep(sleepMillis);
            remainMillis -= sleepMillis;
        }
    }

    @Override
    public void join() throws InterruptedException {
        this.wait(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
    }

    @Override
    public void stop() {
        this.lossLeadership();
        this.selector.close();
        this.client.close();
    }
}
