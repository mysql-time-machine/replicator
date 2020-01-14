package com.booking.replication.it.util

import com.booking.replication.commons.services.ServicesControl
import groovy.sql.Sql

class MySQL {

     static Sql getSqlHandle(
            boolean autoCommit,
            String schemaName,
            ServicesControl mysqlReplicant
        ) {

        def urlReplicant =  new StringBuilder()
                .append("jdbc:mysql://")
                .append(mysqlReplicant.getHost())
                .append(":")
                .append(mysqlReplicant.getPort())
                .append("/")
                .append(schemaName)
                .toString()

        def dbReplicant = [
                url     : urlReplicant,
                user    : 'root',
                password: 'replicator',
                driver  : 'com.mysql.jdbc.Driver'
        ]

        def replicant = Sql.newInstance(
                dbReplicant.url,
                dbReplicant.user,
                dbReplicant.password,
                dbReplicant.driver
        )

        replicant.connection.autoCommit = autoCommit

        return replicant
    }
}
