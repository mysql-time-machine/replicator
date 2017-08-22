package com.booking.replication.replicant;

import java.sql.SQLException;

/**
 * Created by edmitriev on 8/2/17.
 */
public interface ReplicantPool {
    String getReplicantDBActiveHost();

    int getReplicantDBActiveHostServerID();

    int obtainServerID(String host) throws SQLException;
}
