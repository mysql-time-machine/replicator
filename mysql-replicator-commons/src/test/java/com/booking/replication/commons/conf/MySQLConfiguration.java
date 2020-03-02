package com.booking.replication.commons.conf;

import org.testcontainers.containers.Network;

import java.util.Collections;
import java.util.List;

public class MySQLConfiguration {
    private String schema;
    private String username;
    private String password;
    private String confPath;
    private List<String> initScripts;
    private Network network;
    private String networkAlias;

    private final String DEFAULT_SCHEMA     = "test";
    private final String DEFAULT_USERNAME   = "replicator";
    private final String DEFAULT_PASSWORD   = "replicator";
    private final String DEFAULT_CONF_PATH  = "my.cnf";
    private final Network DEFAULT_NETWORK   = null;
    private final String DEFAULT_NETWORK_ALIAS = "";
    private final List<String> DEFAULT_INIT_SCRIPTS = Collections.emptyList();


    public MySQLConfiguration(String schema,
                              String username,
                              String password,
                              String confPath,
                              List<String> initScripts,
                              Network network,
                              String networkAlias) {
        this.schema = schema;
        this.username = username;
        this.password = password;
        this.confPath = confPath;
        this.initScripts = initScripts;
        this.network = network;
        this.networkAlias = networkAlias;
    }

    public MySQLConfiguration() {
        this.schema   = DEFAULT_SCHEMA;
        this.username = DEFAULT_USERNAME;
        this.password = DEFAULT_PASSWORD;
        this.confPath = DEFAULT_CONF_PATH;
        this.initScripts = DEFAULT_INIT_SCRIPTS;
        this.network     = DEFAULT_NETWORK;
        this.networkAlias= DEFAULT_NETWORK_ALIAS;
    }

    public MySQLConfiguration (List<String> initScripts) {
        this();
        this.initScripts = initScripts;
    }

    public Network getNetwork() {
        return network;
    }

    public String getNetworkAlias() {
        return networkAlias;
    }

    public String getSchema() {
        return schema;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public String getConfPath() {
        return confPath;
    }

    public List<String> getInitScripts() {
        return initScripts;
    }
}
