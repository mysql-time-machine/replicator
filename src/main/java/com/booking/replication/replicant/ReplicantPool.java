package com.booking.replication.replicant;

import java.sql.SQLException;

/**
 * Created by edmitriev on 8/2/17.
 */
public interface ReplicantPool {
    String getReplicantDBActiveHost();

    long getReplicantDBActiveHostServerID();

    long obtainServerID(String host) throws SQLException;
}
