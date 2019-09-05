package com.booking.replication.commons.services.containers.mysql;

import com.booking.replication.commons.conf.MySQLConfiguration;

public class MySQLContainerProvider {

    private MySQLContainerProvider() {
    }

    public static MySQLContainer startWithMySQLConfiguration(final MySQLConfiguration configuration) {
        final MySQLContainer mySQLContainer = MySQLContainer.newInstance().withMySQLConfiguration(configuration);
        mySQLContainer.doStart();
        return mySQLContainer;
    }
}
