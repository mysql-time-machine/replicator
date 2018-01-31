package com.booking.replication.applier.hbase;

import com.booking.replication.applier.EventApplier;
import com.booking.replication.mysql.binlog.model.*;
import com.booking.replication.mysql.binlog.model.augmented.AugmentedEventData;
import com.booking.replication.mysql.binlog.model.augmented.TableEventData;
import com.booking.replication.mysql.binlog.model.transaction.TransactionEventData;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

@SuppressWarnings("unused")
public class HBaseEventApplier implements EventApplier {
    interface Configuration {
        String SCHEMA = "hbase.schema";
        String ZOOKEEPER_QUORUM = "hbase.zookeeper.quorum";
    }

    private static final Logger log = Logger.getLogger(HBaseEventApplier.class.getName());

    private static final String DIGEST_ALGORITHM = "MD5";
    private static final byte[] CF = "d".getBytes();

    private final Connection connection;
    private final String schema;

    public HBaseEventApplier(Map<String, String> configuration) {
        try {
            String zookeeperQuorum = configuration.get(Configuration.ZOOKEEPER_QUORUM);
            String schema = configuration.get(Configuration.SCHEMA);

            Objects.requireNonNull(zookeeperQuorum, String.format("Configuration required: %s", Configuration.ZOOKEEPER_QUORUM));
            Objects.requireNonNull(schema, String.format("Configuration required: %s", Configuration.SCHEMA));

            this.connection = this.createConnection(zookeeperQuorum);
            this.schema = schema;
        } catch (IOException exception) {
            throw new UncheckedIOException(exception);
        }
    }

    private Connection createConnection(String zookeeperQuorum) throws IOException {
        org.apache.hadoop.conf.Configuration configuration = HBaseConfiguration.create();

        configuration.set(Configuration.ZOOKEEPER_QUORUM, zookeeperQuorum);

        return ConnectionFactory.createConnection(configuration);
    }

    @Override
    public void accept(Event event) {
        try {
            switch (event.getHeader().getEventType()) {
                case TRANSACTION:
                case AUGMENTED_INSERT:
                case AUGMENTED_UPDATE:
                case AUGMENTED_DELETE:
                    try (Table table = this.getTable(event.getData())) {
                        table.put(this.handleAugmentedPutEvent(event));
                    }
                    break;
                case AUGMENTED_CREATE:
                    this.handleAugmentedCreateEvent(event.getHeader(), event.getData());
                    break;
                case AUGMENTED_ALTER:
                    this.handleAugmentedAlterEvent(event.getHeader(), event.getData());
                    break;
                case AUGMENTED_DROP:
                    this.handleAugmentedDropEvent(event.getHeader(), event.getData());
                    break;
                default:
                    this.handleUnknownEvent(event.getHeader(), event.getData());
                    break;
            }
        } catch (IOException exception) {
            throw new UncheckedIOException(exception);
        } catch (NoSuchAlgorithmException exception) {
            throw new RuntimeException(exception);
        }
    }

    private TableName getTableName(TableEventData data) {
        return TableName.valueOf(String.format("%s.%s", this.schema.toLowerCase(), data.getTableName().toLowerCase()));
    }

    private Table getTable(TableEventData data) throws IOException {
        return this.connection.getTable(this.getTableName(data));
    }

    private byte[] getRowPrimaryKey(AugmentedEventData data, String columnName) throws NoSuchAlgorithmException {
        List<String> rowPrimaryKey = new ArrayList<>();

        for (String primaryKeyColumn : data.getPrimaryKeyColumns()) {
            rowPrimaryKey.add(data.getEventColumns().get(primaryKeyColumn).get(columnName));
        }

        return this.saltRowPrimaryKey(String.join(";", rowPrimaryKey), rowPrimaryKey.get(0)).getBytes();
    }

    private String saltRowPrimaryKey(String hbaseRowID, String firstPartOfRowKey) throws NoSuchAlgorithmException {
        byte[] bytesMD5 = MessageDigest.getInstance(
                HBaseEventApplier.DIGEST_ALGORITHM
        ).digest(
                firstPartOfRowKey.getBytes(StandardCharsets.US_ASCII)
        );

        String byte1hex = Integer.toHexString(bytesMD5[0] & 0xFF);
        String byte2hex = Integer.toHexString(bytesMD5[1] & 0xFF);
        String byte3hex = Integer.toHexString(bytesMD5[2] & 0xFF);
        String byte4hex = Integer.toHexString(bytesMD5[3] & 0xFF);

        String salt = ("00" + byte1hex).substring(byte1hex.length())
                    + ("00" + byte2hex).substring(byte2hex.length())
                    + ("00" + byte3hex).substring(byte3hex.length())
                    + ("00" + byte4hex).substring(byte4hex.length());

        return salt + ";" + hbaseRowID;
    }

    private List<Put> handleAugmentedPutEvent(Event event) throws NoSuchAlgorithmException {
        switch (event.getHeader().getEventType()) {
            case TRANSACTION:
                return this.handleTransactionEvent(event.getHeader(), event.getData());
            case AUGMENTED_INSERT:
                return this.handleAugmentedInsertEvent(event.getHeader(), event.getData());
            case AUGMENTED_UPDATE:
                return this.handleAugmentedUpdateEvent(event.getHeader(), event.getData());
            case AUGMENTED_DELETE:
                return this.handleAugmentedDeleteEvent(event.getHeader(), event.getData());
            default:
                return Collections.emptyList();
        }
    }

    private List<Put> handleTransactionEvent(EventHeader header, TransactionEventData data) throws NoSuchAlgorithmException {
        List<Put> putList = new ArrayList<>();

        for (Event event : data.getEvents()) {
            putList.addAll(this.handleAugmentedPutEvent(event));
        }

        return putList;
    }

    private List<Put> handleAugmentedInsertEvent(EventHeader header, AugmentedEventData data) throws NoSuchAlgorithmException {
        Put put = new Put(this.getRowPrimaryKey(data, "value"));

        for (String columnName : data.getTableSchemaVersion().getColumnNames()) {
            String columnValue = data.getEventColumns().get(columnName).get("value");

            if (columnValue == null) {
                columnValue = "NULL";
            }

            put.addColumn(
                    HBaseEventApplier.CF,
                    columnName.getBytes(),
                    header.getTimestamp(),
                    columnValue.getBytes()
            );
        }

        put.addColumn(
                HBaseEventApplier.CF,
                "row_status".getBytes(),
                header.getTimestamp(),
                "I".getBytes()
        );

        return Collections.singletonList(put);
    }

    private List<Put> handleAugmentedUpdateEvent(EventHeader header, AugmentedEventData data) throws NoSuchAlgorithmException {
        Put put = new Put(this.getRowPrimaryKey(data, "value_after"));

        for (String columnName : data.getTableSchemaVersion().getColumnNames()) {
            String columnValueBefore = data.getEventColumns().get(columnName).get("value_before");
            String columnValueAfter = data.getEventColumns().get(columnName).get("value_after");

            if ((columnValueBefore != null && columnValueAfter == null) ||
                (columnValueBefore == null && columnValueAfter != null) ||
                !columnValueBefore.equals(columnValueAfter)) {
                put.addColumn(
                        HBaseEventApplier.CF,
                        columnName.getBytes(),
                        header.getTimestamp(),
                        columnValueAfter.getBytes()
                );
            }
        }

        put.addColumn(
                HBaseEventApplier.CF,
                "row_status".getBytes(),
                header.getTimestamp(),
                "U".getBytes()
        );

        return Collections.singletonList(put);
    }

    private List<Put> handleAugmentedDeleteEvent(EventHeader header, AugmentedEventData data) throws NoSuchAlgorithmException {
        Put put = new Put(this.getRowPrimaryKey(data, "value"));

        put.addColumn(
                HBaseEventApplier.CF,
                "row_status".getBytes(),
                header.getTimestamp(),
                "D".getBytes()
        );

        return Collections.singletonList(put);
    }

    private void handleAugmentedCreateEvent(EventHeader header, AugmentedEventData data) throws IOException {
    }

    private void handleAugmentedAlterEvent(EventHeader header, AugmentedEventData data) throws IOException {
    }

    private void handleAugmentedDropEvent(EventHeader header, AugmentedEventData data) throws IOException {
    }

    private void handleUnknownEvent(EventHeader header, EventData data) {
        HBaseEventApplier.log.log(Level.FINE, "Unknown event type {}", header.getEventType().name());
    }

    @Override
    public void close() throws IOException {
        if (this.connection != null) {
            this.connection.close();
        }
    }
}
