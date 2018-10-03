package com.booking.replication.spec

import com.booking.replication.ReplicatorIntegrationTest
import com.booking.replication.commons.services.ServicesControl
import com.fasterxml.jackson.databind.ObjectMapper
import groovy.sql.Sql
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes

class HBaseTransmitInsertsSpec implements ReplicatorIntegrationTest {

    private String HBASE_COLUMN_FAMILY_NAME = "d"

    private String SCHEMA_NAME = "replicator"

    private String TABLE_NAME = "sometable"

    private static final ObjectMapper MAPPER = new ObjectMapper()

    // TODO: move to ServiceProvider in common; split common to test-utils and common
    Sql getReplicantSql(boolean autoCommit, ServicesControl mysqlReplicant) {

        def urlReplicant =  new StringBuilder()
                .append("jdbc:mysql://")
                .append(mysqlReplicant.getHost())
                .append(":")
                .append(mysqlReplicant.getPort())
                .append("/")
                .append(SCHEMA_NAME)
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

    @Override
    void doAction(ServicesControl mysqlReplicant) {

        // get handle
        def replicantMySQLHandle = getReplicantSql(
                false,
                mysqlReplicant// <- autoCommit
        )

        // create table
        def sqlCreate = """
        CREATE TABLE
            sometable (
            pk_part_1         varchar(5) NOT NULL DEFAULT '',
            pk_part_2         int(11)    NOT NULL DEFAULT 0,
            randomInt         int(11)             DEFAULT NULL,
            randomVarchar     varchar(32)         DEFAULT NULL,
            PRIMARY KEY       (pk_part_1,pk_part_2),
            KEY randomVarchar (randomVarchar),
            KEY randomInt     (randomInt)
            ) ENGINE=InnoDB DEFAULT CHARSET=latin1;
        """

        replicantMySQLHandle.execute(sqlCreate);
        replicantMySQLHandle.commit();

        // INSERT
        def testRows = [
                ['A', '1', '665726', 'PZBAAQSVoSxxFassQEAQ'],
                ['B', '2', '490705', 'cvjIXQiWLegvLs kXaKH'],
                ['C', '3', '437616', 'pjFNkiZExAiHkKiJePMp'],
                ['D', '4', '537616', 'SjFNkiZExAiHkKiJePMp'],
                ['E', '5', '637616', 'ajFNkiZExAiHkKiJePMp']
        ]

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
                        randomVarchar
                )
                values (
                        ${row[0]},
                        ${row[1]},
                        ${row[2]},
                        ${row[3]}
                )
                """
                    replicantMySQLHandle.execute(sqlString)
                    replicantMySQLHandle.commit()
                } catch (Exception ex) {
                    replicantMySQLHandle.rollback()
                }
        }

//        // SELECT CHECK
//        def resultSet = []
//        replicantMySQLHandle.eachRow('select * from sometable') {
//            row ->
//                resultSet.add([
//                        pk_part_1    : row.pk_part_1,
//                        pk_part_2    : row.pk_part_2,
//                        randomInt    : row.randomInt,
//                        randomVarchar: row.randomVarchar
//                ])
//        }
//        print("retrieved from Replicant: " + prettyPrint(toJson(resultSet)))

        replicantMySQLHandle.close()
    }

    @Override
    boolean actualEqualsExpected(Object expected, Object retrieved) {

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

    @Override
    Object getExpectedState() {
        def expected = new TreeMap<>()
        def timestamp = 0
        def f = HBASE_COLUMN_FAMILY_NAME
        [
                "0d61f837;C;3|${f}:pk_part_1|${timestamp}|C",
                "0d61f837;C;3|${f}:pk_part_2|${timestamp}|3",
                "0d61f837;C;3|${f}:randomInt|${timestamp}|437616",
                "0d61f837;C;3|${f}:randomVarchar|${timestamp}|pjFNkiZExAiHkKiJePMp",
                "0d61f837;C;3|${f}:row_status|${timestamp}|I",
                "3a3ea00c;E;5|${f}:pk_part_1|${timestamp}|E",
                "3a3ea00c;E;5|${f}:pk_part_2|${timestamp}|5",
                "3a3ea00c;E;5|${f}:randomInt|${timestamp}|637616",
                "3a3ea00c;E;5|${f}:randomVarchar|${timestamp}|ajFNkiZExAiHkKiJePMp",
                "3a3ea00c;E;5|${f}:row_status|${timestamp}|I",
                "7fc56270;A;1|${f}:pk_part_1|${timestamp}|A",
                "7fc56270;A;1|${f}:pk_part_2|${timestamp}|1",
                "7fc56270;A;1|${f}:randomInt|${timestamp}|665726",
                "7fc56270;A;1|${f}:randomVarchar|${timestamp}|PZBAAQSVoSxxFassQEAQ",
                "7fc56270;A;1|${f}:row_status|${timestamp}|I",
                "9d5ed678;B;2|${f}:pk_part_1|${timestamp}|B",
                "9d5ed678;B;2|${f}:pk_part_2|${timestamp}|2",
                "9d5ed678;B;2|${f}:randomInt|${timestamp}|490705",
                "9d5ed678;B;2|${f}:randomVarchar|${timestamp}|cvjIXQiWLegvLs kXaKH",
                "9d5ed678;B;2|${f}:row_status|${timestamp}|I",
                "f623e75a;D;4|${f}:pk_part_1|${timestamp}|D",
                "f623e75a;D;4|${f}:pk_part_2|${timestamp}|4",
                "f623e75a;D;4|${f}:randomInt|${timestamp}|537616",
                "f623e75a;D;4|${f}:randomVarchar|${timestamp}|SjFNkiZExAiHkKiJePMp",
                "f623e75a;D;4|${f}:row_status|${timestamp}|I"
        ].collect({ x ->
            def r = x.tokenize('|')
            if (expected[r[0]] == null) { expected[r[0]] = new TreeMap<>() }

            if (expected[r[0]][r[1]] == null) { expected[r[0]][r[1]] = new TreeMap() }

            expected[r[0]][r[1]][r[2]] = r[3]
        })

        def grouped = new TreeMap()
        grouped["sometable"] = expected
        return grouped
    }

     @Override
     Object getActualState() throws IOException {

        String tableName = TABLE_NAME
        def data = new TreeMap<>()
        try {
            // config
            Configuration config = HBaseConfiguration.create()
            Connection connection = ConnectionFactory.createConnection(config)
            Table table = connection.getTable(TableName.valueOf(Bytes.toBytes(tableName)))

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

                    if (columnName == "transaction_uuid" || columnName == "transaction_xid") {
                        continue
                    }

                    String fullColumnName = Bytes.toString(cell.getFamily()) + ":" + columnName

                    if (data[tableName] == null) {
                        data[tableName] = new TreeMap<>()
                    }
                    if (data[tableName][rowKey] == null) {
                        data[tableName][rowKey] = new TreeMap<>()
                    }
                    if (data[tableName][rowKey][fullColumnName] == null) {
                        data[tableName][rowKey][fullColumnName] = new TreeMap<>()
                    }

                    data.get(tableName).get(rowKey).get(fullColumnName).put(
                        cell.getTimestamp(), Bytes.toString(cell.getValue())
                    )
                }
            }
        } catch (IOException e) {
            e.printStackTrace()
        }
        return data
    }
}
