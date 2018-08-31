package com.booking.replication.spec;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.HashMap;

public class BasicHBaseTransmitSpec {

    private String HBASE_COLUMN_FAMILY_NAME = "d";

    public HashMap<String,HashMap<String, HashMap<Long, String>>> retrieveReplicatedDataFromHBase(
            String fromTableName) throws IOException {

        HashMap<String,HashMap<String, HashMap<Long, String>>> data = new HashMap<>();

        try {
            // instantiate Configuration class
            Configuration config = HBaseConfiguration.create();

            Connection connection = ConnectionFactory.createConnection(config);

            Table table = connection.getTable(TableName.valueOf(Bytes.toBytes(fromTableName)));

            // read
            Scan scan = new Scan();
            scan.setMaxVersions(1000);
            ResultScanner scanner = table.getScanner(scan);
            for (Result row : scanner) {

                CellScanner cs =  row.cellScanner();
                while (cs.advance()) {
                    Cell cell = cs.current();
                    String columnName = Bytes.toString(cell.getQualifierArray());
                    data.computeIfAbsent(fromTableName, tn -> new HashMap<>());
                    data.get(fromTableName).computeIfAbsent(columnName, cn -> new HashMap<>());
                    data.get(fromTableName).get(columnName).put(
                            cell.getTimestamp(), Bytes.toString(cell.getValueArray())
                    );
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return data;
    }
}
