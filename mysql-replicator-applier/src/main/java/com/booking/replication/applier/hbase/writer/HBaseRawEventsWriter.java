package com.booking.replication.applier.hbase.writer;

import com.booking.replication.augmenter.model.event.AugmentedEvent;
import com.booking.replication.augmenter.model.event.DeleteRowsAugmentedEventData;
import com.booking.replication.augmenter.model.event.UpdateRowsAugmentedEventData;
import com.booking.replication.augmenter.model.event.WriteRowsAugmentedEventData;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.time.Instant;
import java.util.*;

// Dummy applier for testing: writes raw events, grouped by table name
// with rowKey as timestamp
public class HBaseRawEventsWriter implements HBaseApplierWriter {

    private static final Logger LOG = LogManager.getLogger(HBaseRawEventsWriter.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final int FLUSH_RETRY_LIMIT = 30;
    private long bufferClearTime = 0L;
    private final int DATA_FORMAT = 0;
    private final String HBASE_COLUMN_DEFAULT_FAMILY_NAME = "d";

    Connection connection;
    Admin admin;
    Collection<AugmentedEvent> buffered;

    public HBaseRawEventsWriter(Configuration hbaseConfig, Map<String, Object> configuration) throws IOException {
        connection = ConnectionFactory.createConnection(hbaseConfig);
        admin = connection.getAdmin();
        buffered = new ArrayList<>();
    }

    @Override
    public void buffer(Collection<AugmentedEvent> events) {
        for (AugmentedEvent event: events) {
            buffered.add(event);
        }
    }

    @Override
    public long getBufferClearTime() {
        return bufferClearTime;
    }

    @Override
    public int getBufferSize() {
        return  buffered.size();
    }

    @Override
    public boolean flush() {
        boolean result = false;
        try {
            result = flushWithRetry(); // false means all retries have failed
            buffered.clear();
            bufferClearTime = Instant.now().toEpochMilli();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return  result;
    }

    private Boolean flushWithRetry() throws InterruptedException {

        int counter = FLUSH_RETRY_LIMIT;

        while (counter > 0) {
            counter--;

            try {
                writeToHBase(buffered);
                return true;
            } catch (IOException e) {
                LOG.warn("Failed to write to HBase.", e);
            }
            Thread.sleep(1000); // TODO: exponential backoff
        }
        return false;
    }

    private void writeToHBase(Collection<AugmentedEvent> events) throws IOException {

        // TODO: throw new IOException("Chaos Monkey");

        Map<String,List<Put>> mutationsByTable = generateMutations(events);

        for (String tableName : mutationsByTable.keySet()) {

            Table table = connection.getTable(TableName.valueOf(Bytes.toBytes(tableName)));

            table.put(mutationsByTable.get(tableName));
        }
    }

    private Map<String, List<Put>> generateMutations(Collection<AugmentedEvent> events) throws IOException {

        Map<String, List<Put>> putsByTable = new HashMap<>();

        for (AugmentedEvent event : events) {
            String tableName = getTableName(event);
            String value = MAPPER.writeValueAsString(event);
            Long timestamp = event.getHeader().getTimestamp();

            Put put = generateMutation(tableName, timestamp, value);

            if (!putsByTable.containsKey(tableName)) {
                putsByTable.put(tableName, new ArrayList<>());
            }
            putsByTable.get(tableName).add(put);
        }
        return putsByTable;
    }

    private Put generateMutation(String tableNameString, Long timestamp, String json) throws IOException {

        TableName tableName = TableName.valueOf(tableNameString);

        // check table exists
        if (!admin.tableExists(tableName)) {

            HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
            HColumnDescriptor cd = new HColumnDescriptor(HBASE_COLUMN_DEFAULT_FAMILY_NAME);

            cd.setMaxVersions(1000);
            tableDescriptor.addFamily(cd);
            tableDescriptor.setCompactionEnabled(true);

            admin.createTable(tableDescriptor);
            LOG.info("created table " + tableNameString);

        }

        // create Put
        String rowKey = new StringBuilder(String.valueOf(timestamp)).reverse().toString();
        // String rowKey = randString() + ";" + timestamp.toString();

        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(
            Bytes.toBytes(HBASE_COLUMN_DEFAULT_FAMILY_NAME),
            Bytes.toBytes("event"),
            timestamp,
            Bytes.toBytes(json)
        );

        return put;
    }

    private String getTableName(AugmentedEvent event) {
        switch (event.getHeader().getEventType()) {
            case WRITE_ROWS:
                return ((WriteRowsAugmentedEventData)event.getData()).getEventTable().getName();
            case UPDATE_ROWS:
                return ((UpdateRowsAugmentedEventData)event.getData()).getEventTable().getName();
            case DELETE_ROWS:
                return ((DeleteRowsAugmentedEventData)event.getData()).getEventTable().getName();
            default:
                return null;
        }
    }

    public static String randString() {

        int leftLimit = 97; // letter 'a'
        int rightLimit = 122; // letter 'z'
        int targetStringLength = 3;
        Random random = new Random();
        StringBuilder buffer = new StringBuilder(targetStringLength);
        for (int i = 0; i < targetStringLength; i++) {
            int randomLimitedInt = leftLimit + (int)
                    (random.nextFloat() * (rightLimit - leftLimit + 1));
            buffer.append((char) randomLimitedInt);
        }
        String generatedString = buffer.toString();

        return generatedString;
    }
}

