package zookeeper.impl;

import com.booking.replication.Configuration;
import com.booking.replication.checkpoints.LastVerifiedBinlogFile;
import com.booking.replication.checkpoints.SafeCheckPoint;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import zookeeper.ZookeeperTalk;

import java.io.Closeable;
import java.io.IOException;

/**
 * Created by bosko on 5/31/16.
 */
public class ZookeeperTalkImpl implements ZookeeperTalk {
    private static final Logger LOGGER = LoggerFactory.getLogger(ZookeeperTalkImpl.class);

    private final Configuration configuration;

    private volatile boolean isLeader = false;
    private volatile boolean isRunning = true;

    private SafeCheckPoint safeCheckPoint;

    private CuratorFramework client;

    private class myle extends LeaderSelectorListenerAdapter implements Closeable {

        private final Runnable callback;
        private final LeaderSelector leaderSelector;

        public myle(CuratorFramework client, String path, Runnable onLeadership) {
            super();
            leaderSelector = new LeaderSelector(client, path, this);

            callback = onLeadership;
        }

        public void start() {
            leaderSelector.start();
        }

        @Override
        public void takeLeadership(CuratorFramework curatorFramework) throws Exception {
            isLeader = true;
            LOGGER.info("Acquired leadership, starting Replicator.");

            try {
                callback.run();
            } catch(Exception e) {
                e.printStackTrace();
            }
            isRunning = false;
        }

        @Override
        public void close() throws IOException {
            leaderSelector.close();
        }
    }


    public ZookeeperTalkImpl(Configuration configuration) throws Exception {
        this.configuration = configuration;

        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);

        String cluster = configuration.getZookeeperQuorum();
        if (cluster == null || "".equals(cluster))
            throw new Exception("expecting env ZOOKEEPER_CLUSTER (should be in /etc/sysconfig/bookings.puppet)");

        client = CuratorFrameworkFactory.newClient(cluster, retryPolicy);
        client.start();

        client.createContainers(configuration.getZookeeperPath());
    }

    synchronized public boolean onLeaderElection(Runnable callback) throws InterruptedException {
        LOGGER.info("Waiting to become a leader.");

        myle le = new myle(client, String.format("%s/master", configuration.getZookeeperPath()), callback);
        le.start();

        while(!isLeader || isRunning) {
            Thread.sleep(1000);
        }

        // TODO: get this from zookeeper
        return true;
    }

    @Override
    public void storeSafeCheckPointInZK(SafeCheckPoint safeCheckPoint) {
        // TODO: store in zk
        this.safeCheckPoint = safeCheckPoint;
    }

    @Override
    public SafeCheckPoint getSafeCheckPointFromZK() {
        // TODO: get from zk
        SafeCheckPoint safeCheckPoint = new LastVerifiedBinlogFile();
        safeCheckPoint.setSafeCheckPointMarker(configuration.getStartingBinlogFileName());
        return safeCheckPoint;
    }

}
