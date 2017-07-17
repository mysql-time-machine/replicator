package com.booking.replication.replicant;

import java.sql.SQLException;

/**
 * Created by edmitriev on 8/2/17.
 */
public class DummyReplicantPool implements ReplicantPool {
    @Override
    public String getReplicantDBActiveHost() {
        return "localhost";
    }

    @Override
    public int getReplicantDBActiveHostServerID() {
        return 0;
    }

    @Override
    public int obtainServerID(String host) throws SQLException {
        return 0;
    }
}
