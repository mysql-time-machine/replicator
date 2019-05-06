package com.booking.replication.it.util;

import com.booking.replication.commons.conf.MySQLConfiguration;
import com.booking.replication.commons.services.ServicesControl;
import com.booking.replication.supplier.mysql.binlog.BinaryLogSupplier;
import com.mysql.jdbc.Driver;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testcontainers.shaded.org.apache.commons.lang.text.StrSubstitutor;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.sql.*;
import java.util.*;


public class MySQLRunner {
    private static final Logger LOG = LogManager.getLogger(MySQLRunner.class);
    private static final String CONNECTION_URL_FORMAT = "jdbc:mysql://%s:%d/%s";

    public boolean runMysqlScript(ServicesControl mysql,
                                  MySQLConfiguration configuration,
                                  String scriptFilePath,
                                  Map<String, String> scriptParams,
                                  boolean runAsRoot) {
        Statement statement;
        BasicDataSource dataSource = initDatasource(mysql, configuration, Driver.class.getName(), runAsRoot);
        try (Connection connection = dataSource.getConnection()) {
            statement = connection.createStatement();
            LOG.info("Executing query from " + scriptFilePath);
            String s;
            StringBuilder sb = new StringBuilder();

            FileReader fr = new FileReader(new File(scriptFilePath));
            BufferedReader br = new BufferedReader(fr);
            while ((s = br.readLine()) != null) {
                sb.append(s);
            }
            br.close();

            StrSubstitutor sub = new StrSubstitutor(scriptParams, "{", "}");
            String subSb = sub.replace(sb);

            String[] inst = subSb.split(";");
            for (String query : inst) {
                if (!query.trim().equals("")) {
                    statement.execute(query);
                    LOG.debug("Query executed - " + query);
                }
            }
            return true;
        } catch (Exception exception) {
            LOG.warn(String.format("error executing query \"%s\": %s",
                    scriptFilePath, exception.getMessage()));
            return false;
        }

    }

    public List<HashMap<String,Object>> runMysqlQuery(ServicesControl mysql,
                               MySQLConfiguration configuration,
                               String query,
                               boolean runAsRoot) {
        Statement statement;
        BasicDataSource dataSource = initDatasource(mysql, configuration, Driver.class.getName(), runAsRoot);
        try (Connection connection = dataSource.getConnection()) {
            statement = connection.createStatement();
            LOG.debug("Executing query - " + query);

            if (!query.trim().equals("")) {
                statement.execute(query);
            }

            ResultSet result = statement.getResultSet();
            return convertResultSetToList(result);
        } catch (Exception exception) {
            LOG.warn(String.format("error executing query \"%s\": %s",
                    query, exception.getMessage()));
            return null;
        }

    }

    private List<HashMap<String,Object>> convertResultSetToList(ResultSet rs) throws SQLException {
        ResultSetMetaData md = rs.getMetaData();
        int columns = md.getColumnCount();
        List<HashMap<String,Object>> list = new ArrayList<>();

        while (rs.next()) {
            HashMap<String,Object> row = new HashMap<>(columns);
            for(int i=1; i<=columns; ++i) {
                row.put(md.getColumnName(i),rs.getObject(i));
            }
            list.add(row);
        }

        return list;
    }

    private BasicDataSource initDatasource(ServicesControl mysql,
                                           MySQLConfiguration configuration,
                                           Object driverClass,
                                           boolean runAsRoot) {
        String hostname = mysql.getHost();
        Object port = mysql.getPort();
        String schema = configuration.getSchema();
        String username = runAsRoot ? "root" : configuration.getUsername();
        String password = configuration.getPassword();

        Objects.requireNonNull(hostname, String.format("Configuration required: %s", BinaryLogSupplier.Configuration.MYSQL_HOSTNAME));
        Objects.requireNonNull(schema, String.format("Configuration required: %s", BinaryLogSupplier.Configuration.MYSQL_SCHEMA));
        Objects.requireNonNull(username, String.format("Configuration required: %s", BinaryLogSupplier.Configuration.MYSQL_USERNAME));
        Objects.requireNonNull(password, String.format("Configuration required: %s", BinaryLogSupplier.Configuration.MYSQL_PASSWORD));

        return this.getDataSource(driverClass.toString(),
                hostname,
                Integer.parseInt(port.toString()),
                schema,
                username,
                password);
    }

    private BasicDataSource getDataSource(String driverClass, String hostname, int port, String schema, String username, String password) {
        BasicDataSource dataSource = new BasicDataSource();

        dataSource.setDriverClassName(driverClass);
        dataSource.setUrl(String.format(CONNECTION_URL_FORMAT, hostname, port, schema));
        dataSource.setUsername(username);
        dataSource.setPassword(password);

        return dataSource;
    }
}
