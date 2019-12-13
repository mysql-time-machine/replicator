package com.booking.replication.it.hbase.impl

import com.booking.replication.applier.hbase.StorageConfig
import com.booking.replication.augmenter.AugmenterFilter
import com.booking.replication.augmenter.model.AugmenterModel
import com.booking.replication.it.hbase.ReplicatorHBasePipeline
import com.booking.replication.commons.services.ServicesControl
import com.booking.replication.it.hbase.ReplicatorHBasePipelineSpec
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
 * Tests the table name suffix removal merge filter
 * */
class TableNameMergeFilterIT implements ReplicatorHBasePipeline  {

        private static final ObjectMapper MAPPER = new ObjectMapper()

        private String SCHEMA_NAME = "replicator"

        private final tableName1 = "table_merge_test_201811"
        private final tableName2 = "table_merge_test_201812"
        private final tableNameMerged = "table_merge_test"

        private static final String AUGMENTER_FILTER_TYPE = "TABLE_MERGE_PATTERN";
        private static final String AUGMENTER_FILTER_CONFIGURATION = "([_][12]\\d{3}(0[1-9]|1[0-2]))";

        @Override
        Map<String, Object> perTestConfiguration(Map<String,Object> genericConfig) {
            println "...TableNameMergeFilterTestImpl.getConfiguration() called, setting FILTER_TYPE and FILTER_CONFIGURATION"
            genericConfig.put(AugmenterFilter.Configuration.FILTER_TYPE, AUGMENTER_FILTER_TYPE);
            genericConfig.put(AugmenterFilter.Configuration.FILTER_CONFIGURATION, AUGMENTER_FILTER_CONFIGURATION);
            return genericConfig;
        }

        @Override
        String testName() {
            return "TableNameMergeFilterTestImpl"
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
            def sqlCreate1 = sprintf("""
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
                """, tableName1
            )

            replicantMySQLHandle.execute(sqlCreate1)
            replicantMySQLHandle.commit()


            def sqlCreate2 = sprintf("""
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
                """, tableName2
            )

            replicantMySQLHandle.execute(sqlCreate2)
            replicantMySQLHandle.commit()

            // INSERT
            def columns = "(pk_part_1,pk_part_2,randomInt,randomVarchar)"
            replicantMySQLHandle.execute(sprintf(
                    "insert into %s %s values ('user',42,1,'xx')", tableName1, columns
            ))
            replicantMySQLHandle.commit()

            replicantMySQLHandle.execute(sprintf(
                    "insert into %s %s values ('user',43,2,'yy')", tableName2, columns
            ))
            replicantMySQLHandle.commit()

            replicantMySQLHandle.close()
        }

        @Override
        Object getExpectedState() {
            return [
                    "1|xx",
                    "2|yy"
            ]
        }

        @Override
        Object getActualState() throws IOException {

            def ri = []
            def rvc = []

            try {
                // config
                StorageConfig storageConfig = StorageConfig.build(HBase.getConfiguration())
                Configuration config = storageConfig.getConfig()
                Connection connection = ConnectionFactory.createConnection(config)

                Table table = connection.getTable(TableName.valueOf(
                        Bytes.toBytes(ReplicatorHBasePipelineSpec.HBASE_TARGET_NAMESPACE),
                        Bytes.toBytes(tableNameMerged)))

                // read
                Scan scan = new Scan()
                scan.setMaxVersions(1000)
                ResultScanner scanner = table.getScanner(scan)
                for (Result row : scanner) {

                    CellScanner cs =  row.cellScanner()
                    while (cs.advance()) {
                        Cell cell = cs.current()

                        String columnName =  Bytes.toString(cell.getQualifier())

                        if (columnName ==
                                AugmenterModel.Configuration.UUID_FIELD_NAME
                                ||
                                columnName ==
                                AugmenterModel.Configuration.XID_FIELD_NAME) {
                            continue
                        }

                        if (columnName == "randomInt") {
                            ri.add(Bytes.toString(cell.getValue()))
                        }

                        if (columnName == "randomVarchar") {
                            rvc.add(Bytes.toString(cell.getValue()))
                        }
                    }
                }
                table.close();
            } catch (IOException e) {
                e.printStackTrace()
            }

            def result = [
                    ri[0] + "|" + rvc[0],
                    ri[1] + "|" + rvc[1]
            ]
            return result
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
