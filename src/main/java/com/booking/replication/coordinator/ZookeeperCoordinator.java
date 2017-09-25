package com.booking.replication.coordinator;

import com.booking.replication.Configuration;
import com.booking.replication.checkpoints.LastCommittedPositionCheckpoint;
import com.booking.replication.checkpoints.SafeCheckPoint;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;

/**
 * Created by bosko on 5/31/16.
 */
public class ZookeeperCoordinator implements CoordinatorInterface {
    private static final Logger LOGGER = LoggerFactory.getLogger(ZookeeperCoordinator.class);

    private final Configuration configuration;

    private volatile boolean isRunning = true;

    private SafeCheckPoint safeCheckPoint;

    private CuratorFramework client;

    private class CoordinatorLeaderElectionListener extends LeaderSelectorListenerAdapter implements Closeable {

        private final Runnable callback;
        private final LeaderSelector leaderSelector;

        public CoordinatorLeaderElectionListener(CuratorFramework client, String path, Runnable onLeadership) {
            super();
            leaderSelector = new LeaderSelector(client, path, this);

            callback = onLeadership;
        }

        public void start() {
            leaderSelector.start();
        }

        @Override
        public void takeLeadership(CuratorFramework curatorFramework) throws Exception {
            LOGGER.info("Acquired leadership, starting Replicator.");

            try {
                callback.run();
            } catch (Exception e) {
                e.printStackTrace();
            }

            // Lost leadership
            isRunning = false;
        }

        @Override
        public void close() throws IOException {
            leaderSelector.close();
        }
    }

    /**
     * Zookeeper-based Coordinator implementation.
     *
     * @param configuration Replicator configuration
     */
    public ZookeeperCoordinator(Configuration configuration)  {
        this.configuration = configuration;
        this.checkPointPath = String.format("%s/checkpoint", configuration.getZookeeperPath());

        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);

        String cluster = configuration.getZookeeperQuorum();

        client = CuratorFrameworkFactory.newClient(cluster, retryPolicy);
        client.start();

        try {
            client.createContainers(configuration.getZookeeperPath());
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(String.format(
                "Failed to create Zookeeper Coordinator container at path: %s (Because of: %s)",
                configuration.getZookeeperPath(),
                e.getMessage()
            ));
        }
    }

    /**
     * Callback function on aquiring the leader lock.
     * @param callback              Callback.
     * @throws InterruptedException This may happen if we lose leadership in the event of zookeeper disconnect.
     */
    public synchronized void onLeaderElection(Runnable callback) throws InterruptedException {
        LOGGER.info("Waiting to become a leader.");

        CoordinatorLeaderElectionListener le = new CoordinatorLeaderElectionListener(
                client,
                String.format("%s/master", configuration.getZookeeperPath()),
                callback);

        le.start();

        while (isRunning) {
            Thread.sleep(1000);
        }
    }

    private final ObjectMapper mapper = new ObjectMapper();

    @Override
    public String serialize(SafeCheckPoint checkPoint) throws JsonProcessingException {
        return mapper.writeValueAsString(checkPoint);
    }

    private final String checkPointPath;

    @Override
    public void storeSafeCheckPoint(SafeCheckPoint safeCheckPoint) throws Exception {

        try {
            String serializedCP = serialize(safeCheckPoint);

            Stat exists = client.checkExists().forPath(checkPointPath);
            if ( exists != null ) {
                client.setData().forPath(checkPointPath, serializedCP.getBytes());
            } else {
                client.create().withMode(CreateMode.PERSISTENT).forPath(checkPointPath, serializedCP.getBytes());
            }
            LOGGER.info(String.format("Stored information in ZK: %s", serializedCP));
        } catch (JsonProcessingException e) {
            LOGGER.error("Failed to serialize safeCheckPoint!");
            throw e;
        } catch (Exception e ) {
            e.printStackTrace();
            throw e;
        }
    }

    @Override
    public LastCommittedPositionCheckpoint getSafeCheckPoint() {
        try {
            if (client.checkExists().forPath(checkPointPath) == null) {
                LOGGER.warn("Could not find metadata in zookeeper.");
                return null;
            }
            byte[] data = client.getData().forPath(checkPointPath);
            return mapper.readValue(data, LastCommittedPositionCheckpoint.class);
        } catch (JsonProcessingException e) {
            LOGGER.error(String.format("Failed to deserialize checkpoint data. %s", e.getMessage()));
            e.printStackTrace();
        } catch (Exception e) {
            LOGGER.error(String.format("Got an error while reading metadata from Zookeeper: %s", e.getMessage()));
            e.printStackTrace();
        }

        return null;
    }

}
