package com.booking.replication.it.hbase.impl

import com.booking.replication.applier.hbase.StorageConfig
import com.booking.replication.augmenter.model.AugmenterModel;
import com.booking.replication.it.hbase.ReplicatorHBasePipelineIntegrationTest
import com.booking.replication.applier.hbase.time.RowTimestampOrganizer
import com.booking.replication.commons.services.ServicesControl
import com.booking.replication.it.hbase.ReplicatorHBasePipelineIntegrationTestRunner
import com.booking.replication.it.util.HBase
import com.booking.replication.it.util.MySQL
import com.fasterxml.jackson.databind.ObjectMapper

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.*
import org.apache.hadoop.hbase.client.*
import org.apache.hadoop.hbase.util.Bytes

/**
 * Inserts multiple rows and verifies that the order of
 * microsecond timestamps in HBase is the same as
 * the order in which the rows were inserted
 */
class MicrosecondValidationTestImpl implements ReplicatorHBasePipelineIntegrationTest {

    private String HBASE_COLUMN_FAMILY_NAME = "d"

    private String SCHEMA_NAME = "replicator"

    private String TABLE_NAME = "tbl_microseconds_test"

    private Integer NR_ABOVE_MAX_VERSIONS = 5

    private static final ObjectMapper MAPPER = new ObjectMapper()

    @Override
    String testName() {
        return "HBaseMicrosecondValidation"
    }

    @Override
    boolean actualEqualsExpected(Object actual, Object expected) {

        def exp = (List<List<String>>) expected
        def act = (List<List<String>>) actual

        def retrievedIntVal = act.get(0)
        def expectedIntVal = exp.get(0)

        // sorted decs, so first item is last version
        def retFirst = retrievedIntVal.remove(0)
        def expFirst = expectedIntVal.remove(0)

        // all should be equal except the last version
        return(
            retrievedIntVal
                    .join("|")
                    .equals(expectedIntVal.join("|"))
            &&
            (retFirst != expFirst)
        )
    }

    @Override
    void doAction(ServicesControl mysqlReplicant) {

        // get handle
        def replicant = MySQL.getSqlHandle(
                false,
                SCHEMA_NAME,
                mysqlReplicant
        )

        // CREATE
        def sqlCreate = sprintf("""
        CREATE TABLE
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
                "insert into %s %s values ('user',42,0,'c0')", TABLE_NAME, columns
        ))

        def where = " where pk_part_1 = 'user' and pk_part_2 = 42"

        def cap = RowTimestampOrganizer.TIMESTAMP_SPAN_MICROSECONDS + NR_ABOVE_MAX_VERSIONS + 1

        for (int i=1; i < cap; i ++) {
            replicant.execute(sprintf(
                    "update %s set randomInt = %d, randomVarchar = 'c%s' %s", TABLE_NAME, i, i, where
            ))
        }

        replicant.commit()
        replicant.close()
    }

    @Override
    Object getExpectedState() {

        def range = (RowTimestampOrganizer.TIMESTAMP_SPAN_MICROSECONDS..0)

        def l = range.toList()
        def r = range.toList().collect({ x -> "c${x}"})

        // all should be equal except the last version
        l.remove(0)
        r.remove(0)

        return [
            l.join("|"),
            r.join("|")
        ].join("|")
    }

    @Override
    Object getActualState() throws IOException {

        String tableName = TABLE_NAME

        def l = []
        def r = []

        try {
            // config
            StorageConfig storageConfig = StorageConfig.build(HBase.getConfiguration())
            Configuration config = storageConfig.getConfig()
            Connection connection = ConnectionFactory.createConnection(config)

            Table table = connection.getTable(TableName.valueOf(
                    Bytes.toBytes(ReplicatorHBasePipelineIntegrationTestRunner.HBASE_TARGET_NAMESPACE),
                    Bytes.toBytes(tableName))
            )

            // read
            Scan scan = new Scan()
            scan.setMaxVersions(1000)
            ResultScanner scanner = table.getScanner(scan)
            for (Result row : scanner) {
                CellScanner cs =  row.cellScanner()
                while (cs.advance()) {
                    Cell cell = cs.current()
                    String rowKey = Bytes.toString(cell.getRow())
                    String columnName =  Bytes.toString(cell.getQualifier())

                    if (rowKey != 'ee11cbb1;user;42') {
                        continue
                    }

                    if (columnName ==
                            AugmenterModel.Configuration.UUID_FIELD_NAME
                         ||
                         columnName ==
                            AugmenterModel.Configuration.XID_FIELD_NAME) {
                        continue
                    }

                    if (columnName == "randomInt") {
                        l.add(Bytes.toString(cell.getValue()))
                    }

                    if (columnName == "randomVarchar") {
                        r.add(Bytes.toString(cell.getValue()))
                    }

                }
            }
            table.close();
        } catch (IOException e) {
            e.printStackTrace()
        }

        l.remove(0)
        r.remove(0)

        return [
                l.join("|"),
                r.join("|")
        ].join("|")
    }
}

