package zookeeper.impl;

import com.booking.replication.Configuration;
import com.booking.replication.checkpoints.LastVerifiedBinlogFile;
import com.booking.replication.checkpoints.SafeCheckPoint;
import zookeeper.ZookeeperTalk;

/**
 * Created by bosko on 5/31/16.
 */
public class ZookeeperTalkImpl implements ZookeeperTalk {

    private final Configuration configuration;

    private  SafeCheckPoint safeCheckPoint;

    public ZookeeperTalkImpl(Configuration configuration) {
        this.configuration = configuration;
    }

    @Override
    public boolean amIALeader() {
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
