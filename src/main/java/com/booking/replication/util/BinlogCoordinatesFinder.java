package com.booking.replication.util;

import com.booking.replication.sql.QueryInspector;
import com.booking.replication.sql.exception.QueryInspectorException;
import org.apache.commons.dbcp2.BasicDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

/**
 * Created by psalimov on 9/22/16.
 */
public class BinlogCoordinatesFinder {

    private static final Logger LOGGER = LoggerFactory.getLogger(BinlogCoordinatesFinder.class);

    public static class BinlogCoordinates {
        private String fileName;
        private long position;

        public BinlogCoordinates(String fileName, long position) {
            this.fileName = fileName;
            this.position = position;
        }

        public String getFileName() {
            return fileName;
        }

        public long getPosition() {
            return position;
        }
    }

    private final String host;
    private final int port;
    private final String username;
    private final String password;

    private final QueryInspector queryInspector;


    public BinlogCoordinatesFinder(String host, int port, String username, String password, QueryInspector queryInspector) {
        this.host = host;
        this.port = port;
        this.username = username;
        this.password = password;
        this.queryInspector = queryInspector;
    }

    public BinlogCoordinates findCoordinates(String gtid) {

        BasicDataSource source = new BasicDataSource();

        source.setDriverClassName("com.mysql.jdbc.Driver");

        source.setUsername(username);
        source.setPassword(password);

        source.setUrl( String.format("jdbc:mysql://%s:%s", host, port) );

        source.addConnectionProperty("useUnicode", "true");
        source.addConnectionProperty("characterEncoding", "UTF-8");

        try ( Connection connection = source.getConnection() ){

            String[] files = getBinaryLogs(connection);

            String file = findFile(gtid, files, connection);
            long position = findPosition(gtid, file, connection);

            return new BinlogCoordinates(file,position);

        } catch (SQLException | QueryInspectorException e) {

            LOGGER.error("Failed to find binlog coordinates for gtid ", e);

            throw new RuntimeException(e);

        }

    }

    private long findPosition(final String gtid, String file, Connection connection) throws QueryInspectorException, SQLException {

        ResultSet rs = findEvent(resultSet-> {

                try {
                    String query = resultSet.getString( "Info" );
                    return queryInspector.isPseudoGTID(query) && gtid.equals( queryInspector.extractPseudoGTID(query) );
                } catch (SQLException | QueryInspectorException e) {
                    throw new RuntimeException(e);
                }

            }, file, connection);

        if (rs == null) throw new RuntimeException(String.format("Binlog file %s does not contain given GTID", file));

        return rs.getLong("Pos");

    }

    private ResultSet findEvent(Predicate<ResultSet> condition, String file, Connection connection) throws SQLException {

        try ( PreparedStatement statement = connection.prepareStatement("SHOW BINLOG EVENTS IN \"?\" LIMIT ?,?")){

            int start = 0;
            int limit = 500;

            for (;;){

                statement.setString(1, file);
                statement.setInt(2,start);
                statement.setInt(3,limit);

                try ( ResultSet results = statement.executeQuery() ) {

                    boolean empty = true;

                    while (results.next()) {

                        empty = false;

                        if (condition.test(results)) return results;

                        start += limit;

                    }

                    if (empty) return null;

                }

            }

        }

    }

    private String findFile(String gtid, String[] files, Connection connection) throws QueryInspectorException, SQLException {

        int l = 0;
        int h = files.length - 1;

        int cmp;

        if ( gtid.compareToIgnoreCase( getFirstGTID( files[h], connection ) ) >= 0 ) return files[h];

        cmp = gtid.compareToIgnoreCase( getFirstGTID( files[l], connection ) );

        if ( cmp < 0 ) {
            throw new RuntimeException("No binlog file contain the given GTID ");
        } else if ( cmp ==0 ){
            return files[l];
        }

        // we maintain invariant GTID(l) < gtid < GTID(h) and we need files[i] such that GTID(i) <= gtid < GTID(i+1)
        while ( h - l > 1){
            int m = l + ( h - l) / 2; // l < m < h

            cmp = gtid.compareToIgnoreCase( getFirstGTID( files[m], connection ) );

            if (cmp == 0) {

                return files[m];

            } else if (cmp > 0){

                l = m; // maintain gtid > GTID(l)

            } else {

                h = m; // maintain gtid < GTID(h)

            }

        }

        // h = l + 1, GTID(l) < gtid < GTID(h)

        return files[l];
    }

    private String getFirstGTID(String file, Connection connection) throws SQLException, QueryInspectorException {

        LOGGER.info(String.format("Getting first GTID from %s...", file));

        ResultSet rs = findEvent( resultSet -> {

                try {
                    return queryInspector.isPseudoGTID( resultSet.getString( "Info" ) );
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }

            } , file, connection);

        if (rs == null) throw new RuntimeException(String.format("Binlog file %s does not contain any GTID", file));

        String firstGtid = queryInspector.extractPseudoGTID( rs.getString( "Info" ) );

        LOGGER.info(String.format("First GTID in %s is %s", file, firstGtid));

        return firstGtid;
    }

    private String[] getBinaryLogs( Connection connection ) throws SQLException{

        try ( Statement statement = connection.createStatement();
              ResultSet result = statement.executeQuery("SHOW BINARY LOGS;") ){

            List<String> files = new ArrayList<>();

            while ( result.next() ){
                files.add( result.getString("Log_name") );
            }

            return files.toArray(new String[files.size()]);

        }

    }


}
