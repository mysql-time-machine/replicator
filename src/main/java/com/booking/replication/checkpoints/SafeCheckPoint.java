package com.booking.replication.checkpoints;

/**
 * Created by bosko on 5/30/16.
 */
public interface SafeCheckPoint {

    public int getCheckpointType();

    public void setSafeCheckPointMarker(String marker);

    public String getSafeCheckPointMarker();

    public String toJSON();
}
