package com.booking.replication.it.util

import com.booking.replication.applier.hbase.StorageConfig
import com.booking.replication.it.hbase.ReplicatorHBasePipelineIntegrationTestRunner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

class HBase {

    private static  Map<String, Object> configuration

    static setConfiguration( Map<String, Object> cnf) {
        configuration = cnf
    }

    static Map<String, Object> getConfiguration() {
        return configuration
    }

    static Map<String, Map<String, Map<String,String>>> scanHBaseTable(String tableName) {

        def data = new TreeMap()

        try {
            // config
            StorageConfig storageConfig = StorageConfig.build(this.getConfiguration())
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

                CellScanner cs = row.cellScanner()
                while (cs.advance()) {
                    Cell cell = cs.current()

                    String rowKey = Bytes.toString(cell.getRow())

                    String columnName = Bytes.toString(cell.getQualifier())

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
                            cell.getTimestamp().toString(), Bytes.toString(cell.getValue())
                    )
                }
            }
            table.close();
        } catch (IOException e) {
            e.printStackTrace()
        }

        def tableData = data[tableName]
        return tableData
    }
}
