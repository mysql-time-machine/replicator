package zookeeper;

import com.booking.replication.checkpoints.SafeCheckPoint;

/**
 * Created by bosko on 5/30/16.
 */
public interface ZookeeperTalk {

    public boolean amIALeader();

    public void storeSafeCheckPointInZK(SafeCheckPoint safeCheckPoint);

    public SafeCheckPoint getSafeCheckPointFromZK();
}
