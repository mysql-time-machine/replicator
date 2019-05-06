package com.booking.replication.commons.conf;

import org.testcontainers.containers.Network;

import java.util.List;

public class MySQLConfiguration {
    private String schema;
    private String username;
    private String password;
    private String confPath;
    private List<String> initScripts;
    private Network network;
    private String networkAlias;

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
