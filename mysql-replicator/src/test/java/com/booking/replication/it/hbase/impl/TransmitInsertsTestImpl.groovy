package com.booking.replication.it.hbase.impl

import com.booking.replication.applier.hbase.StorageConfig
import com.booking.replication.augmenter.model.AugmenterModel
import com.booking.replication.it.hbase.ReplicatorHBasePipelineIntegrationTest
import com.booking.replication.commons.services.ServicesControl
import com.booking.replication.it.hbase.ReplicatorHBasePipelineIntegrationTestRunner
import com.booking.replication.it.util.HBase
import com.booking.replication.it.util.MySQL
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*
import org.apache.hadoop.hbase.client.*
import org.apache.hadoop.hbase.util.Bytes

import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneId
import java.time.ZoneOffset
import java.time.ZonedDateTime

class TransmitInsertsTestImpl implements ReplicatorHBasePipelineIntegrationTest {

    private String HBASE_COLUMN_FAMILY_NAME = "d"

    private String SCHEMA_NAME = "replicator"

    private String TABLE_NAME = "sometable"

    private static final ObjectMapper MAPPER = new ObjectMapper()

    def testRows = [
            ['A', '1', '665726', 'PZBAAQSVoSxxFassQEAQ', '1990-01-01', '2018-07-14T12:00:00'],
            ['B', '2', '490705', 'cvjIXQiWLegvLs kXaKH', '1991-01-01', '1991-01-01T13:00:00'],
            ['C', '3', '437616', 'pjFNkiZExAiHkKiJePMp', '1992-01-01', '1992-01-01T14:00:00'],
            ['D', '4', '537616', 'SjFNkiZExAiHkKiJePMp', '1993-01-01', '1993-01-01T15:00:00'],
            ['E', '5', '637616', 'ajFNkiZExAiHkKiJePMp', '1994-01-01', '1994-01-01T16:00:00']
    ]

    @Override
    void doAction(ServicesControl mysqlReplicant) {

        // get handle
        def replicantMySQLHandle = MySQL.getSqlHandle(
                false,
                SCHEMA_NAME,
                mysqlReplicant
        )

        // create table
        def sqlCreate = """
        CREATE TABLE
            sometable (
            
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

        replicantMySQLHandle.execute(sqlCreate);
        replicantMySQLHandle.commit();

        // insert
        testRows.each {
            row ->
                try {
                    def sqlString = """
                INSERT INTO
                sometable (
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
                    replicantMySQLHandle.execute(sqlString)
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
        return "HBaseTransmitInserts"
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

        def expected = new TreeMap<>()
        def f = HBASE_COLUMN_FAMILY_NAME
        [
                "7fc56270;A;1|${f}:pk_part_1|A",
                "7fc56270;A;1|${f}:pk_part_2|1",
                "7fc56270;A;1|${f}:randomInt|665726",
                "7fc56270;A;1|${f}:randomVarchar|PZBAAQSVoSxxFassQEAQ",
                "7fc56270;A;1|${f}:randomDate|1990-01-01",
                "7fc56270;A;1|${f}:aTimestamp|${getExpectedTimestamp(testRows[0][5])}",
                "7fc56270;A;1|${f}:row_status|I",

                "9d5ed678;B;2|${f}:pk_part_1|B",
                "9d5ed678;B;2|${f}:pk_part_2|2",
                "9d5ed678;B;2|${f}:randomInt|490705",
                "9d5ed678;B;2|${f}:randomVarchar|cvjIXQiWLegvLs kXaKH",
                "9d5ed678;B;2|${f}:randomDate|1991-01-01",
                "9d5ed678;B;2|${f}:aTimestamp|${getExpectedTimestamp(testRows[1][5])}",
                "9d5ed678;B;2|${f}:row_status|I",

                "0d61f837;C;3|${f}:pk_part_1|C",
                "0d61f837;C;3|${f}:pk_part_2|3",
                "0d61f837;C;3|${f}:randomInt|437616",
                "0d61f837;C;3|${f}:randomVarchar|pjFNkiZExAiHkKiJePMp",
                "0d61f837;C;3|${f}:randomDate|1992-01-01",
                "0d61f837;C;3|${f}:aTimestamp|${getExpectedTimestamp(testRows[2][5])}",
                "0d61f837;C;3|${f}:row_status|I",

                "f623e75a;D;4|${f}:pk_part_1|D",
                "f623e75a;D;4|${f}:pk_part_2|4",
                "f623e75a;D;4|${f}:randomInt|537616",
                "f623e75a;D;4|${f}:randomVarchar|SjFNkiZExAiHkKiJePMp",
                "f623e75a;D;4|${f}:randomDate|1993-01-01",
                "f623e75a;D;4|${f}:aTimestamp|${getExpectedTimestamp(testRows[3][5])}",
                "f623e75a;D;4|${f}:row_status|I",

                "3a3ea00c;E;5|${f}:pk_part_1|E",
                "3a3ea00c;E;5|${f}:pk_part_2|5",
                "3a3ea00c;E;5|${f}:randomInt|637616",
                "3a3ea00c;E;5|${f}:randomVarchar|ajFNkiZExAiHkKiJePMp",
                "3a3ea00c;E;5|${f}:randomDate|1994-01-01",
                "3a3ea00c;E;5|${f}:aTimestamp|${getExpectedTimestamp(testRows[4][5])}",
                "3a3ea00c;E;5|${f}:row_status|I"
        ].collect({ x ->
            def r = x.tokenize('|')

            if (expected[r[0]] == null) { expected[r[0]] = new TreeMap<>() }

            expected[r[0]][r[1]] = r[2]
        })

        def grouped = new TreeMap()
        grouped["sometable"] = expected
        return grouped
    }

    @Override
    Object getActualState() throws IOException {

        String NAMESPACE = ReplicatorHBasePipelineIntegrationTestRunner.HBASE_TARGET_NAMESPACE
        String tableName = TABLE_NAME

        def data = new TreeMap<>()
        try {
            // config
            StorageConfig storageConfig = StorageConfig.build(HBase.getConfiguration())
            Configuration config = storageConfig.getConfig()
            Connection connection = ConnectionFactory.createConnection(config)

            Table table = connection.getTable(
                    TableName.valueOf(
                            Bytes.toBytes(NAMESPACE),
                            Bytes.toBytes(tableName)
                    )
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

                    if (columnName ==
                            AugmenterModel.Configuration.UUID_FIELD_NAME
                            ||
                            columnName ==
                            AugmenterModel.Configuration.XID_FIELD_NAME) {
                        continue
                    }

                    String fullColumnName = Bytes.toString(cell.getFamily()) + ":" + columnName

                    if (data[tableName] == null) {
                        data[tableName] = new TreeMap<>()
                    }
                    if (data[tableName][rowKey] == null) {
                        data[tableName][rowKey] = new TreeMap<>()
                    }

                    data.get(tableName).get(rowKey).put(fullColumnName, Bytes.toString(cell.getValue())
                    )
                }
                table.close();
            }
        } catch (IOException e) {
            e.printStackTrace()
        }
        return data
    }
}
