package com.booking.replication.replicant;

/**
 * Created by bosko on 9/12/16.
 */
public class ReplicantActiveHost {

    private final String host;
    private final long    serverID;

    public ReplicantActiveHost(String host, long serverID) {
        this.host = host;
        this.serverID = serverID;
    }

    public String getHost() {
        return host;
    }

    public long getServerID() {
        return serverID;
    }
}
