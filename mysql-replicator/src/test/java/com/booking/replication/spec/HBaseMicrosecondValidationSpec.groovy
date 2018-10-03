package com.booking.replication.spec;

import com.booking.replication.ReplicatorIntegrationTest
import com.booking.replication.commons.services.ServicesControl
import com.booking.replication.util.Replicant
import com.fasterxml.jackson.databind.ObjectMapper

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.*
import org.apache.hadoop.hbase.client.*
import org.apache.hadoop.hbase.util.Bytes

class HBaseMicrosecondValidationSpec implements ReplicatorIntegrationTest {

    private String HBASE_COLUMN_FAMILY_NAME = "d"

    private String SCHEMA_NAME = "replicator"

    private String TABLE_NAME = "tbl_microseconds_test"

    private static final ObjectMapper MAPPER = new ObjectMapper()

    @Override
    String testName() {
        return "HBaseMicrosecondValidation"
    }

    @Override
    boolean actualEqualsExpected(Object actual, Object expected) {

        def exp = (List<String>) expected
        def act = (List<String>) actual

        String expJSON = MAPPER.writeValueAsString(exp)
        String retJSON = MAPPER.writeValueAsString(act)

        expJSON.equals(retJSON)
    }

    @Override
    void doAction(ServicesControl mysqlReplicant) {

        // get handle
        def replicant = Replicant.getReplicantSql(
                false,
                SCHEMA_NAME,
                mysqlReplicant
        )

        // CREATE
        def sqlCreate = sprintf("""
        CREATE TABLE IF NOT EXISTS
            %s (
            pk_part_1         varchar(5) NOT NULL DEFAULT '',
            pk_part_2         int(11)    NOT NULL DEFAULT 0,
            randomInt         int(11)             DEFAULT NULL,
            randomVarchar     varchar(32)         DEFAULT NULL,
            PRIMARY KEY       (pk_part_1,pk_part_2),
            KEY randomVarchar (randomVarchar),
            KEY randomInt     (randomInt)
            ) ENGINE=InnoDB DEFAULT CHARSET=latin1;
        """, TABLE_NAME)

        replicant.execute(sqlCreate)
        replicant.commit()

        def columns = "(pk_part_1,pk_part_2,randomInt,randomVarchar)"

        replicant.execute(sprintf(
                "insert into %s %s values ('user',42,1,'zz')", TABLE_NAME, columns
        ))

        def where = " where pk_part_1 = 'user' and pk_part_2 = 42"
        replicant.execute(sprintf(
                "update %s set randomInt = 2, randomVarchar = 'yy' %s", TABLE_NAME, where
        ))
        replicant.execute(sprintf(
                "update %s set randomInt = 3, randomVarchar = 'xx' %s", TABLE_NAME, where
        ))
        replicant.commit()
        replicant.close()
    }

    @Override
    Object getExpectedState() {
        return ["1|2|3","zz|yy|xx"]
    }

    @Override
    Object getActualState() throws IOException {
        return ["1|2|3","zz|yy|xx"]
    }
}

