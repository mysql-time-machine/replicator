package com.booking.replication.spec

import com.booking.replication.ReplicatorIntegrationTest
import com.booking.replication.commons.services.ServicesControl
import com.booking.replication.runner.ReplicatorIntegrationTestRunner
import com.booking.replication.util.HBase
import com.booking.replication.util.Replicant
import groovy.sql.Sql

/**
 * This test verifies that we get the payloads injected into transactions and
 * that we get them in the same order (when sorted by timestamp) in which
 * transactions were made
 * */
class HBasePayloadTableSpec implements ReplicatorIntegrationTest {

    private tableName = "tbl_payload_data"
    private payloadTableName =
            ReplicatorIntegrationTestRunner.HBASE_TEST_PAYLOAD_TABLE_NAME

    private String SCHEMA_NAME = "replicator"

    @Override
    String testName() {
        return "HBasePayloadTableSpec"
    }

    @Override
    void doAction(ServicesControl mysqlReplicant) {

        def replicantMySQLHandle = Replicant.getReplicantSql(
            false,
            SCHEMA_NAME,
            mysqlReplicant
        )

        createPayloadTable(replicantMySQLHandle)

        // CREATE
        def sqlCreate = sprintf("""
        CREATE TABLE
            %s (
            pk          varchar(5) NOT NULL DEFAULT '',
            val         int(11)             DEFAULT NULL,
            PRIMARY KEY       (pk)
            ) ENGINE=InnoDB DEFAULT CHARSET=latin1;
        """, tableName)

        replicantMySQLHandle.execute(sqlCreate)
        replicantMySQLHandle.commit()

        // begin 1, insert & update
        def columns = "(pk,val)"
        replicantMySQLHandle.execute(sprintf(
                "insert into %s %s values ('first',1)", tableName, columns
        ))
        def where = " where pk = 'first'"
        replicantMySQLHandle.execute(sprintf(
                "update %s set val = 2 %s", tableName, where
        ))

        replicantMySQLHandle.execute(
                sprintf(
                        "insert into %s (event_id,server_role,strange_int) values ('aabbcc','admin',7)",
                        payloadTableName
                ))
        replicantMySQLHandle.commit()

        // begin 2, update
        replicantMySQLHandle.execute(sprintf(
                "update %s set val = 12 %s", tableName, where
        ))
        def payloadSQL = sprintf('''
            insert into %s (event_id,server_role,strange_int)
            values ('aabbdd','client',17)
        ''', payloadTableName)

        replicantMySQLHandle.execute(payloadSQL)
        replicantMySQLHandle.commit()

        replicantMySQLHandle.close()

    }

    @Override
    Object getExpectedState() {
        return ["d:event_id|aabbcc}{d:server_role|admin}{d:strange_int|7",
                "d:event_id|aabbdd}{d:server_role|client}{d:strange_int|17"]
    }

    @Override
    Object getActualState() throws IOException {

        // map { $rowKey => { $fullColumnName => { $timestamp => $value }}}
        def data = HBase.scanHBaseTable(tableName)
        def payload = HBase.scanHBaseTable(payloadTableName)

        def transactionUUIDsSortedByTimestampInDataTable =
                data['8b04d5e3;first']['d:transaction_uuid']
                        .sort{ it.key }
                        .collect {
                            timestamp, transactionUUID  ->
                                transactionUUID
        }

        def sortedUnique = []
        def processed = [].toSet()
        for (String uuid : transactionUUIDsSortedByTimestampInDataTable) {
            if (processed.contains(uuid)) {
                continue
            } else {
                sortedUnique.add(uuid)
                processed.add(uuid)
            }
        }

        def payloadVals = sortedUnique.collect { transactionUUID ->
            def r = [:]
            payload[transactionUUID].each { columnName,v ->      // (k,v) is (columnName, {timestamp => value})
                r[columnName] = columnName + "|" + v.values()[0] // "column_name|column_value"
            }
            r
        }

        return payloadVals.collect {
            it["d:event_id"]+"}{"+it["d:server_role"]+"}{"+it["d:strange_int"]
        }

    }

    @Override
    boolean actualEqualsExpected(Object actual, Object expected) {
        def exp = (List<String>) expected
        def act = (List<String>) actual

        boolean ok = true
        exp.eachWithIndex{
            String entry, int i ->
                if (!entry.equals(act[i])) { ok = false }
        }

        return ok
    }

    void createPayloadTable(Sql replicantMySQLHandle) {
        def sqlCreate = sprintf('''
        create table %s (
            event_id char(6) not null,
            server_role varchar(255) not null,
            strange_int int not null,
            primary key (event_id)
        ) ENGINE = BLACKHOLE
        ''', payloadTableName)
        replicantMySQLHandle.execute(sqlCreate)
        replicantMySQLHandle.commit()
    }
}
