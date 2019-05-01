package com.booking.replication.it.hbase.impl

import com.booking.replication.applier.hbase.StorageConfig
import com.booking.replication.augmenter.model.AugmenterModel
import com.booking.replication.it.hbase.ReplicatorHBasePipelineIntegrationTest
import com.booking.replication.commons.services.ServicesControl
import com.booking.replication.it.hbase.ReplicatorHBasePipelineIntegrationTestRunner
import com.booking.replication.it.util.HBase
import com.booking.replication.it.util.MySQL
import com.fasterxml.jackson.databind.ObjectMapper
import groovy.sql.Sql
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*
import org.apache.hadoop.hbase.client.*
import org.apache.hadoop.hbase.util.Bytes

import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneId
import java.time.ZoneOffset
import java.time.ZonedDateTime

class TableWhiteListTest implements ReplicatorHBasePipelineIntegrationTest {

    private String HBASE_COLUMN_FAMILY_NAME = "d"

    private String SCHEMA_NAME = "replicator"

    private String TABLE_NAME_INCLUDED = "sometable_included"
    private String TABLE_NAME_EXCLUDED = "sometable_excluded"

    private static final ObjectMapper MAPPER = new ObjectMapper()

    def testRows = [
            ['A', '1', '665726', 'PZBAAQSVoSxxFassQEAQ', '1990-01-01', '2018-07-14T12:00:00'],
            ['B', '2', '490705', 'cvjIXQiWLegvLs kXaKH', '1991-01-01', '1991-01-01T13:00:00'],
            ['C', '3', '437616', 'pjFNkiZExAiHkKiJePMp', '1992-01-01', '1992-01-01T14:00:00'],
            ['D', '4', '537616', 'SjFNkiZExAiHkKiJePMp', '1993-01-01', '1993-01-01T15:00:00'],
            ['E', '5', '637616', 'ajFNkiZExAiHkKiJePMp', '1994-01-01', '1994-01-01T16:00:00']
    ]

    void createTableWithDefaultSchema(Sql mysqlHandle, String tableName) {

        def sqlCreate = """
            CREATE TABLE """ + tableName + """ (
            
                pk_part_1         varchar(5) NOT NULL DEFAULT '',
                pk_part_2         int(11)    NOT NULL DEFAULT 0,
                
                randomInt         int(11)             DEFAULT NULL,
                randomVarchar     varchar(32)         DEFAULT NULL,
                
                randomDate        date                DEFAULT NULL,
                aTimestamp        timestamp           DEFAULT CURRENT_TIMESTAMP,
                
                PRIMARY KEY       (pk_part_1,pk_part_2),
                KEY randomVarchar (randomVarchar),
                
                KEY randomInt     (randomInt)
                
            ) ENGINE=InnoDB DEFAULT CHARSET=latin1;
        """

        mysqlHandle.execute(sqlCreate)
        mysqlHandle.commit()

    }

    @Override
    void doAction(ServicesControl mysqlReplicant) {

        // get handle
        def replicantMySQLHandle = MySQL.getSqlHandle(
                false,
                SCHEMA_NAME,
                mysqlReplicant
        )

        createTableWithDefaultSchema(replicantMySQLHandle, TABLE_NAME_INCLUDED)
        createTableWithDefaultSchema(replicantMySQLHandle, TABLE_NAME_EXCLUDED)

        // insert to both tables in the same transaction
        testRows[0..2].each {
            row ->
                try {
                    def sqlString1 =
                            """INSERT INTO sometable_included  (
                                pk_part_1,
                                pk_part_2,
                                randomInt,
                                randomVarchar,
                                randomDate,
                                aTimestamp
                        )
                        values (
                                ${row[0]},
                                ${row[1]},
                                ${row[2]},
                                ${row[3]},
                                ${row[4]},
                                ${row[5]}
                        )
                    """

                    def sqlString2 =
                            """INSERT INTO sometable_excluded (
                                pk_part_1,
                                pk_part_2,
                                randomInt,
                                randomVarchar,
                                randomDate,
                                aTimestamp
                        )
                        values (
                                ${row[0]},
                                ${row[1] + 10},
                                ${row[2] + 1},
                                ${row[3]},
                                ${row[4]},
                                ${row[5]}
                        )
                    """

                    replicantMySQLHandle.execute(sqlString1)
                    replicantMySQLHandle.execute(sqlString2)

                    replicantMySQLHandle.commit()

                } catch (Exception ex) {
                    replicantMySQLHandle.rollback()
                }
        }

        // insert to each table in different transaction
        testRows[3..4].each {
            row ->
                try {
                    def sqlString1 =
                                """INSERT INTO sometable_included  (
                                pk_part_1,
                                pk_part_2,
                                randomInt,
                                randomVarchar,
                                randomDate,
                                aTimestamp
                           )
                            values (
                                    ${row[0]},
                                    ${row[1]},
                                    ${row[2]},
                                    ${row[3]},
                                    ${row[4]},
                                    ${row[5]}
                            )
                    """

                    def sqlString2 =
                            """INSERT INTO sometable_excluded (
                            pk_part_1,
                            pk_part_2,
                            randomInt,
                            randomVarchar,
                            randomDate,
                            aTimestamp
                        )
                        values (
                                ${row[0]},
                                ${row[1] + 10},
                                ${row[2] + 1},
                                ${row[3]},
                                ${row[4]},
                                ${row[5]}
                        )
                    """

                    replicantMySQLHandle.execute(sqlString1)
                    replicantMySQLHandle.commit()

                    replicantMySQLHandle.execute(sqlString2)
                    replicantMySQLHandle.commit()

                } catch (Exception ex) {
                    replicantMySQLHandle.rollback()
                }
        }

        replicantMySQLHandle.close()
    }

    @Override
    boolean actualEqualsExpected(Object retrieved, Object expected) {
        expected = (Map<Map<Map<String, String>>>) expected

        retrieved = (Map<Map<Map<String, String>>>) retrieved

        String retJSON = MAPPER.writeValueAsString(retrieved)
        String expJSON = MAPPER.writeValueAsString(expected)

        expJSON.equals(retJSON)
    }

    @Override
    String testName() {
        return "TableWhiteListTest"
    }

    // when running tests in non-GMT timezone
    //      - assuming that at least the java process and mysql are in the same timezone
    private long getExpectedTimestamp(String sentToMySQL) {

        String tzId = ZonedDateTime.now().getZone().toString()
        ZoneId zoneId = ZoneId.of(tzId)
        LocalDateTime aLDT = LocalDateTime.parse(sentToMySQL)
        String offsetString = zoneId.getRules().getOffset(aLDT.atZone(zoneId).toInstant())
        Instant timestamp = aLDT.toInstant(ZoneOffset.of(offsetString))

        return timestamp.toEpochMilli()
    }

    @Override
    Object getExpectedState() {
        return [TABLE_NAME_INCLUDED]
    }

    @Override
    Object getActualState() throws IOException {

        String NAMESPACE = ReplicatorHBasePipelineIntegrationTestRunner.HBASE_TARGET_NAMESPACE

        def data = []

        // config
        StorageConfig storageConfig = StorageConfig.build(HBase.getConfiguration())
        Configuration config = storageConfig.getConfig()
        Connection connection = ConnectionFactory.createConnection(config)
        Admin admin = connection.getAdmin()

        if (admin.tableExists( TableName.valueOf(
                Bytes.toBytes(NAMESPACE),
                Bytes.toBytes(TABLE_NAME_INCLUDED)
        ))) {
            data.add(TABLE_NAME_INCLUDED)
        }

        if (admin.tableExists(TableName.valueOf(
                Bytes.toBytes(NAMESPACE),
                Bytes.toBytes(TABLE_NAME_EXCLUDED)
        ))) {
            data.add(TABLE_NAME_EXCLUDED)
        }


        return data
    }
}
