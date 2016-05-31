package zookeeper;

import com.booking.replication.checkpoints.SafeCheckPoint;

/**
 * Created by bosko on 5/30/16.
 */
public interface ZookeeperTalk {

    public boolean onLeaderElection(Runnable callback) throws InterruptedException;

    public void storeSafeCheckPointInZK(SafeCheckPoint safeCheckPoint);

    public SafeCheckPoint getSafeCheckPointFromZK();
}
