package com.booking.replication.spec

import com.booking.replication.ReplicatorIntegrationTest
import com.booking.replication.commons.services.ServicesControl
import groovy.sql.Sql
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes


class BasicHBaseTransmitSpec implements ReplicatorIntegrationTest {

    private String HBASE_COLUMN_FAMILY_NAME = "d"

    private String SCHEMA_NAME = "replicator"

    private String TABLE_NAME = "sometable"

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
    void doMySQLOps(ServicesControl mysqlReplicant) {

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
//        print("retrieved from MySQL: " + prettyPrint(toJson(resultSet)))

        replicantMySQLHandle.close()
    }

    @Override
    boolean retrievedEqualsExpected(Object expected, Object retrieved) {

        expected = (Map<Map<Map<String, String>>>) expected;

        retrieved = (Map<Map<Map<String, String>>>) retrieved;

        return expected.equals(retrieved)
    }


    @Override
    Object getExpected() {
        def expected = new HashMap<>()
        def data =  [
                "0d61f837;C;3|d:pk_part_1|C",
                "0d61f837;C;3|d:pk_part_2|3",
                "0d61f837;C;3|d:randomint|437616",
                "0d61f837;C;3|d:randomvarchar|pjFNkiZExAiHkKiJePMp",
                "0d61f837;C;3|d:row_status|I",
                "3a3ea00c;E;5|d:pk_part_1|E",
                "3a3ea00c;E;5|d:pk_part_2|5",
                "3a3ea00c;E;5|d:randomint|637616",
                "3a3ea00c;E;5|d:randomvarchar|ajFNkiZExAiHkKiJePMp",
                "3a3ea00c;E;5|d:row_status|I",
                "7fc56270;A;1|d:pk_part_1|A",
                "7fc56270;A;1|d:pk_part_2|1",
                "7fc56270;A;1|d:randomint|665726",
                "7fc56270;A;1|d:randomvarchar|PZBAAQSVoSxxFassQEAQ",
                "7fc56270;A;1|d:row_status|I",
                "9d5ed678;B;2|d:pk_part_1|B",
                "9d5ed678;B;2|d:pk_part_2|2",
                "9d5ed678;B;2|d:randomint|490705",
                "9d5ed678;B;2|d:randomvarchar|cvjIXQiWLegvLs kXaKH",
                "9d5ed678;B;2|d:row_status|I",
                "f623e75a;D;4|d:pk_part_1|D",
                "f623e75a;D;4|d:pk_part_2|4",
                "f623e75a;D;4|d:randomint|537616",
                "f623e75a;D;4|d:randomvarchar|SjFNkiZExAiHkKiJePMp",
                "f623e75a;D;4|d:row_status|I"
        ].collect({ x ->
            def r = x.tokenize('|')
            if (expected[r[0]] == null) { expected[r[0]] = new HashMap() }

            expected[r[0]][r[1]] = r[2]
        })

        def grouped = new HashMap()
        grouped["sometable"] = expected
        return grouped
    }

     @Override
     Object retrieveReplicatedData() throws IOException {

        String tableName = TABLE_NAME
        def data = new HashMap<>()
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

                    String columnName = Bytes.toString(cell.getQualifier())

                    if (data[tableName] == null) {
                        data[tableName] = new HashMap<>()
                    }
                    if (data[tableName][rowKey] == null) {
                        data[tableName][rowKey] = new HashMap<>();
                    }
                    if (data[tableName][rowKey][columnName] == null) {
                        data[tableName][rowKey][columnName] = new HashMap<>()
                    }
                    data.get(tableName).get(rowKey).get(columnName).put(
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
