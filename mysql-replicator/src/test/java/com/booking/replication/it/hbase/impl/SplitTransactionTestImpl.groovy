package com.booking.replication.it.hbase.impl

import com.booking.replication.applier.hbase.StorageConfig
import com.booking.replication.augmenter.model.AugmenterModel
import com.booking.replication.it.hbase.ReplicatorHBasePipelineIntegrationTest
import com.booking.replication.commons.services.ServicesControl
import com.booking.replication.it.hbase.ReplicatorHBasePipelineIntegrationTestRunner
import com.booking.replication.it.util.HBase
import com.booking.replication.it.util.MySQL
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.Cell
import org.apache.hadoop.hbase.CellScanner
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Connection
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.client.ResultScanner
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.client.Table
import org.apache.hadoop.hbase.util.Bytes


/**
 * Implements test for microsecond correctness for a case of a
 * 'split' transaction which begins in one second and commits
 * during the next second
 */
class SplitTransactionTestImpl implements ReplicatorHBasePipelineIntegrationTest  {

    private static final ObjectMapper MAPPER = new ObjectMapper()

    private String SCHEMA_NAME = "replicator"

    private final tableName = "micros_split_transaction_test"

    @Override
    String testName() {
        return "LongTransactionTestImpl"
    }

    @Override
    void doAction(ServicesControl mysqlReplicant) {

        // get handle
        def replicantMySQLHandle = MySQL.getSqlHandle(
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
        """, tableName)

        replicantMySQLHandle.execute(sqlCreate)
        replicantMySQLHandle.commit()

        def columns = "(pk_part_1,pk_part_2,randomInt,randomVarchar)"

        long firstSecond = sleepUntilAndGetNextSecond()

        // commit 10 transactions -> bumps internal transaction ordinal 10 times
        for (int i=1; i < 10; i ++) {
            // INSERT
            replicantMySQLHandle.execute(sprintf(
                    "insert into %s %s values ('user',${i},${i*i},'xx')", tableName, columns
            ))
            replicantMySQLHandle.commit()
        }

        if (firstSecond == java.time.Instant.now().getEpochSecond() ) {
            System.out.println("Still at first second. Beginning long transaction")
        } else {
            System.out.println("Moved to next second before long transaction started" );
            // TODO: handle this case
        }

        //  begin long transaction
        def where = " where pk_part_1 = 'user' and pk_part_2 = 1"

        // UPDATE 1
        replicantMySQLHandle.execute(sprintf(
                "update %s set randomInt = 20, randomVarchar = 'yy' %s", tableName, where
        ))

        //  sleep_until_next_second
        long nextSecond = sleepUntilAndGetNextSecond()
        System.out.println("nextSecond => " + nextSecond);

        // commit long transaction
        // UPDATE 2
        replicantMySQLHandle.execute(sprintf(
                "update %s set randomInt = 30, randomVarchar = 'zz' %s", tableName, where
        ))

        replicantMySQLHandle.commit()

        //  next update
        replicantMySQLHandle.execute(sprintf(
                "update %s set randomInt = 443, randomVarchar = 'aa' %s", tableName, where
        ))

        replicantMySQLHandle.commit()

        if (nextSecond == java.time.Instant.now().getEpochSecond() ) {
            System.out.println("Timeline consistent, all transaction committed by the end of nextSecond")
        }

        replicantMySQLHandle.close()

    }

    long sleepUntilAndGetNextSecond() {
        long current = java.time.Instant.now().getEpochSecond();
        boolean  next = false;

        while (!next) {
            sleep(10)
            long sec = java.time.Instant.now().getEpochSecond();
            if (sec > current) {
                current = sec
                next = true;
            }
        }
        return current;
    }

    @Override
    Object getExpectedState() {
        // expected ordering of values when sorted by microseconds timestamp
        return ["1","20","30","443"]
    }

    @Override
    Object getActualState() throws IOException {
        def data = new TreeMap<>()
        try {
            // config
            StorageConfig storageConfig = StorageConfig.build(HBase.getConfiguration())
            Configuration config = storageConfig.getConfig()

            Connection connection = ConnectionFactory.createConnection(config)
            Table table = connection.getTable(TableName.valueOf(
                    Bytes.toBytes(ReplicatorHBasePipelineIntegrationTestRunner.HBASE_TARGET_NAMESPACE),
                    Bytes.toBytes(tableName)))

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

                    if (columnName != "randomInt") {
                        continue
                    }

                    String fullColumnName = Bytes.toString(cell.getFamily()) + ":" + columnName
                    fullColumnName = fullColumnName.toLowerCase()

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
                            cell.getTimestamp().toString(), Bytes.toString(cell.getValue())
                    )

                }
            }
            table.close();
        } catch (IOException e) {
            e.printStackTrace()
        }

        def dataset = data[tableName]['ee11cbb1;user;1']

        Map<String, Long> ri_cells = dataset['d:randomint']


        def timestamps = ri_cells.keySet().sort();
        def values = []

        for (t in timestamps) {
            values.add(ri_cells[t].toString())
        }

        // ==============================================================
        // Original time line:
        // t_previous (insert & commit)
        // t_previous (begin & update_1)
        // sleep 1s
        // t_next: (update_2 & commit)
        // t_next: (update_3 & commit)
        return values
    }

    @Override
    boolean actualEqualsExpected(Object expected, Object actual) {
        List<String> exp = (List<String>) expected;
        List<String> act = (List<String>) actual;

        String expJSON = MAPPER.writeValueAsString(exp)
        String actJSON = MAPPER.writeValueAsString(act)

        expJSON.equals(actJSON)
    }
}
