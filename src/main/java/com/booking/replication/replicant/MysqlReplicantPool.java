package com.booking.replication.replicant;

import com.booking.replication.Configuration;

import org.apache.commons.dbcp2.BasicDataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;
import java.util.List;

/**
 * Created by bosko on 9/12/16.
 */
public class MysqlReplicantPool implements ReplicantPool {

    private final List<String>        replicantPool;
    private final Configuration       configuration;
    private final ReplicantActiveHost activeHost;

    private static final String GET_SERVER_ID = "SELECT @@server_id";

    private static final Logger LOGGER = LoggerFactory.getLogger(ReplicantPool.class);

    public MysqlReplicantPool(List<String> replicantPool, Configuration configuration) throws Exception {
        this.replicantPool = replicantPool;
        this.configuration = configuration;

        // We set the active host in the pool constructor. That means that the pool list is usable only
        // during startup. Once the active replicant is set, if mysql fails, replicator will not
        // attemt to do failover on its own. It will die. The failover process depends on marathon
        // spawning another docker container with the replicator instance. That second replicator
        // instance will get some other mysql host from the replciant pool. The main thing that
        // replicator does on its own, is figuring out the position from which to continue on another
        // mysql host, using Pseudo GTID stored in zookeeper.
        this.activeHost    = getReplicantActiveHost();
    }

    @Override
    public String getReplicantDBActiveHost() {
        return activeHost.getHost();
    }

    @Override
    public int getReplicantDBActiveHostServerID() {
        return activeHost.getServerID();
    }

    private ReplicantActiveHost getReplicantActiveHost() throws Exception {

        boolean foundGoodHost = false;
        String activeHost;
        int serverID;
        Iterator<String> hostIterator = replicantPool.iterator();

        while (!foundGoodHost) {
            if (hostIterator.hasNext()) {
                String host = hostIterator.next();
                try {
                    serverID = obtainServerID(host);
                    if (serverID != -1) {
                        // got valid server_id
                        activeHost = host;
                        foundGoodHost = true;
                        return new ReplicantActiveHost(activeHost, serverID);
                    }
                } catch (SQLException e) {
                    LOGGER.error("Could not obtain server_id for host " + host + ". Moving to next host in the pool.");
                }
            } else {
                throw new Exception("Replicant pool depleted, no available hosts found!");
            }
        }

        return null;
    }

    @Override
    public int obtainServerID(String host) throws SQLException {

        int serverID = -1;

        BasicDataSource replicantDataSource;

        replicantDataSource = new BasicDataSource();

        replicantDataSource.setDriverClassName("com.mysql.jdbc.Driver");

        String replicantDSN =
            String.format("jdbc:mysql://%s", host);

        replicantDataSource.setUrl(replicantDSN);

        replicantDataSource.addConnectionProperty("useUnicode", "true");
        replicantDataSource.addConnectionProperty("characterEncoding", "UTF-8");
        replicantDataSource.setUsername(configuration.getReplicantDBUserName());
        replicantDataSource.setPassword(configuration.getReplicantDBPassword());

        java.sql.Connection con = replicantDataSource.getConnection();

        Statement         getServerIDStatement         = con.createStatement();
        ResultSet         getServerIDResultSet         = getServerIDStatement.executeQuery(GET_SERVER_ID);
        ResultSetMetaData getServerIDResultSetMetaData = getServerIDResultSet.getMetaData();

        while (getServerIDResultSet.next()) {
            int columnCount = getServerIDResultSetMetaData.getColumnCount();
            if (columnCount != 1) {
                throw new SQLException("SELECT @@server_id result set should have only one column!");
            }
            serverID = getServerIDResultSet.getInt(1);
        }
        getServerIDResultSet.close();
        getServerIDStatement.close();
        con.close();

        return serverID;
    }
}
