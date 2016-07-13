package com.booking.replication.checkpoints;

import java.io.Serializable;

/**
 * Created by bosko on 5/30/16.
 */
public interface SafeCheckPoint extends Serializable {

    public int getCheckpointType();

    public void setSafeCheckPointMarker(String marker);

    public String getSafeCheckPointMarker();

    public String toJson();
}
