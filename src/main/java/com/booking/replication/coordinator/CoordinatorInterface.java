package com.booking.replication.coordinator;

import com.booking.replication.checkpoints.PseudoGTIDCheckpoint;
import com.booking.replication.checkpoints.SafeCheckPoint;

/**
 * Created by bosko on 5/30/16.
 */
public interface CoordinatorInterface {

    public void onLeaderElection(Runnable callback) throws InterruptedException;

    public void storeSafeCheckPoint(SafeCheckPoint safeCheckPoint) throws Exception;

    public PseudoGTIDCheckpoint getSafeCheckPoint();

    public String serialize(SafeCheckPoint checkPoint) throws Exception;
}
