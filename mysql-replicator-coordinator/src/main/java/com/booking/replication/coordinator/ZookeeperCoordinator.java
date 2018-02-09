package com.booking.replication.coordinator;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class ZookeeperCoordinator extends LeaderSelectorListenerAdapter implements Coordinator {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final CuratorFramework client;
    private final LeaderSelector selector;
    private final List<Runnable> takeRunnableList;
    private final List<Runnable> lossRunnableList;
    private final AtomicBoolean hasLeadership;

    ZookeeperCoordinator(Map<String, String> configuration) {
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(
                Integer.parseInt(configuration.getOrDefault(Configuration.RETRY_INITIAL_SLEEP, "1000")),
                Integer.parseInt(configuration.getOrDefault(Configuration.RETRY_MAXIMUM_ATTEMPTS, "3"))
        );

        this.client = CuratorFrameworkFactory.newClient(
                configuration.get(Configuration.CONNECTION_STRING),
                retryPolicy
        );

        this.selector = new LeaderSelector(
                this.client,
                configuration.get(Configuration.LEADERSHIP_PATH),
                this
        );

        this.takeRunnableList = new ArrayList<>();
        this.lossRunnableList = new ArrayList<>();
        this.hasLeadership = new AtomicBoolean();
    }

    @Override
    public <Type> void storeCheckpoint(String path, Type checkpoint) throws IOException {
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
    public <Type> Type loadCheckpoint(String path, Class<Type> type) throws IOException {
        try {
            if (this.client.checkExists().forPath(path) != null) {
                byte[] bytes = this.client.getData().forPath(path);

                if (bytes != null && bytes.length > 0) {
                    return ZookeeperCoordinator.MAPPER.readValue(bytes, type);
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
        try {
            if (!this.hasLeadership.getAndSet(true)) {
                this.takeRunnableList.forEach(Runnable::run);
            }
        } finally {
            if (this.hasLeadership.getAndSet(false)) {
                this.lossRunnableList.forEach(Runnable::run);
            }
        }
    }

    @Override
    public void stateChanged(CuratorFramework client, ConnectionState newState) {
        if (client.getConnectionStateErrorPolicy().isErrorState(newState)) {
            if (this.hasLeadership.getAndSet(false)) {
                this.lossRunnableList.forEach(Runnable::run);
            }
        }
    }

    @Override
    public void onLeadershipTake(Runnable runnable) {
        this.takeRunnableList.add(runnable);
    }

    @Override
    public void onLeadershipLoss(Runnable runnable) {
        this.lossRunnableList.add(runnable);
    }

    @Override
    public String defaultCheckpointPath() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void start() {
        this.client.start();
        this.selector.start();
    }

    @Override
    public void wait(long timeout, TimeUnit unit) throws InterruptedException {
        while (this.client.getState() != CuratorFrameworkState.STOPPED) {
            Thread.sleep(100L);
        }
    }

    @Override
    public void join() throws InterruptedException {
        this.wait(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
    }

    @Override
    public void stop() {
        if (this.hasLeadership.getAndSet(false)) {
            this.lossRunnableList.forEach(Runnable::run);
        }

        this.selector.close();
        this.client.close();
    }
}
