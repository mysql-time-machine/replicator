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

    private static class Holder<T> {

        private T value;

        public T getValue() {
            return value;
        }

        public void setValue(T value) {
            this.value = value;
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

            String file = findFile(gtid, connection);

            long position = findPosition(gtid, file, connection);

            return new BinlogCoordinates(file,position);

        } catch (SQLException | QueryInspectorException e) {

            LOGGER.error("Failed to find binlog coordinates for gtid ", e);

            throw new RuntimeException(e);

        }

    }

    private String findFile(String gtid, Connection connection) throws QueryInspectorException, SQLException {

        String file = MonotonicPartialFunctionSearch.preimageGLB( x -> getFirstGTID( x, connection ), gtid.toUpperCase(), getBinaryLogs(connection) );

        if (file == null) throw new RuntimeException("No binlog file contain the given GTID " + gtid);

        return file;

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

    private long findPosition(final String gtid, String file, Connection connection) throws QueryInspectorException, SQLException {

        final Holder<Long> position = new Holder<>();

        findEvent(resultSet-> {

                try {
                    String query = resultSet.getString( "Info" );

                    if ( queryInspector.isPseudoGTID(query) && gtid.equals( queryInspector.extractPseudoGTID(query) ) ){
                        position.setValue( resultSet.getLong("Pos"));
                        return true;
                    }

                    return false;
                } catch (SQLException | QueryInspectorException e) {
                    throw new RuntimeException(e);
                }

            }, file, connection);

        if ( position.getValue() == null ) throw new RuntimeException(String.format("Binlog file %s does not contain given GTID", file));

        return position.getValue();

    }

    private String getFirstGTID(String file, Connection connection) {

        LOGGER.info(String.format("Getting first GTID from %s...", file));

        final Holder<String> gtidHolder = new Holder<>();

        try {
            findEvent( resultSet -> {

                    try {
                        String query =  resultSet.getString( "Info" );

                        if ( queryInspector.isPseudoGTID( query ) ){

                            gtidHolder.setValue( queryInspector.extractPseudoGTID( query ) );

                            return true;

                        }

                        return false;

                    } catch (SQLException | QueryInspectorException e) {

                        throw new RuntimeException(e);

                    }

                } , file, connection);
        } catch (SQLException e) {

            throw new RuntimeException(e);

        }

        String gtid = gtidHolder.getValue();

        if (gtid != null) gtid = gtid.toUpperCase();

        LOGGER.info(String.format("First GTID in %s is %s", file, gtid));

        return gtid;
    }

    /**
     * Scans events of the binlog file until either the condition becomes true or the end of the file is reached.
     *
     * @param condition the stop scan condition. Should not navigate over the dataset.
     * @param file the file to scan
     * @param connection the connection to use
     * @throws SQLException
     */
    private void findEvent(Predicate<ResultSet> condition, String file, Connection connection) throws SQLException {

        try ( PreparedStatement statement = connection.prepareStatement("SHOW BINLOG EVENTS IN ? LIMIT ?,?")){

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

                        if (condition.test(results)) return;
                    }

                    if (empty) return;

                }

                start += limit;
            }
        }
    }

}
