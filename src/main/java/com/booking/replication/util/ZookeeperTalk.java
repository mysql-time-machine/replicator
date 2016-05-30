package com.booking.replication.util;

/**
 * Created by bosko on 5/30/16.
 */
public interface ZookeeperTalk {

    public boolean amIALeader();

    public void storeSafeCheckPointInZK();

    public void getSafeCheckPointFromZK();
}
