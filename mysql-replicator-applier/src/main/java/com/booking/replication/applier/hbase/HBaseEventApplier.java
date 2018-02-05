package com.booking.replication.applier.hbase;

import com.booking.replication.applier.EventApplier;
import com.booking.replication.model.Event;
import com.booking.replication.model.EventData;
import com.booking.replication.model.EventHeader;
import com.booking.replication.model.augmented.AugmentedEventData;
import com.booking.replication.model.augmented.TableNameEventData;
import com.booking.replication.model.transaction.TransactionEventData;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.RegionSplitter;
import org.jboss.netty.util.internal.ConcurrentHashMap;

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
    public interface Configuration {
        String ZOOKEEPER_QUORUM = "hbase.zookeeper.quorum";
        String SCHEMA = "hbase.schema";
    }

    private static final String DIGEST_ALGORITHM = "MD5";
    private static final byte[] CF = Bytes.toBytes("d");

    private static final Logger log = Logger.getLogger(HBaseEventApplier.class.getName());
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final Map<String, Connection> connections = new ConcurrentHashMap<>();

    private final Connection connection;
    private final String schema;

    public HBaseEventApplier(Map<String, String> configuration) {
        String zookeeperQuorum = configuration.get(Configuration.ZOOKEEPER_QUORUM);
        String schema = configuration.get(Configuration.SCHEMA);

        Objects.requireNonNull(zookeeperQuorum, String.format("Configuration required: %s", Configuration.ZOOKEEPER_QUORUM));
        Objects.requireNonNull(schema, String.format("Configuration required: %s", Configuration.SCHEMA));

        this.connection = this.getConnection(zookeeperQuorum);
        this.schema = schema;
    }

    private Connection getConnection(String zookeeper) {
        return HBaseEventApplier.connections.computeIfAbsent(zookeeper, zookeeperQuorum -> {
            try {
                org.apache.hadoop.conf.Configuration configuration = HBaseConfiguration.create();

                configuration.set("zookeeper.quorum", zookeeperQuorum);

                return ConnectionFactory.createConnection(configuration);
            } catch(IOException exception) {
                throw new UncheckedIOException(exception);
            }
        });
    }

    @Override
    public void accept(Event event) {
        try {
            switch (event.getHeader().getEventType()) {
                case TRANSACTION:
                case AUGMENTED_INSERT:
                case AUGMENTED_UPDATE:
                case AUGMENTED_DELETE:
                    try (Table table = this.getTable(this.getTableName(event.getHeader(), event.getData()))) {
                        table.put(this.handleAugmentedDataEvent(event));
                    }
                    break;
                case AUGMENTED_SCHEMA:
                    this.handleAugmentedSchemaEvent(event.getHeader(), event.getData());
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

    private List<Put> handleAugmentedDataEvent(Event event) throws NoSuchAlgorithmException {
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
            putList.addAll(this.handleAugmentedDataEvent(event));
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
                    Bytes.toBytes(columnName),
                    header.getTimestamp(),
                    Bytes.toBytes(columnValue)
            );
        }

        put.addColumn(
                HBaseEventApplier.CF,
                Bytes.toBytes("row_status"),
                header.getTimestamp(),
                Bytes.toBytes("I")
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
                (columnValueBefore != null && !columnValueBefore.equals(columnValueAfter))) {
                if (columnValueAfter == null) {
                    columnValueAfter = "NULL";
                }

                put.addColumn(
                        HBaseEventApplier.CF,
                        Bytes.toBytes(columnName),
                        header.getTimestamp(),
                        Bytes.toBytes(columnValueAfter)
                );
            }
        }

        put.addColumn(
                HBaseEventApplier.CF,
                Bytes.toBytes("row_status"),
                header.getTimestamp(),
                Bytes.toBytes("U")
        );

        return Collections.singletonList(put);
    }

    private List<Put> handleAugmentedDeleteEvent(EventHeader header, AugmentedEventData data) throws NoSuchAlgorithmException {
        return Collections.singletonList(
                new Put(this.getRowPrimaryKey(data, "value"))
                        .addColumn(
                                HBaseEventApplier.CF,
                                Bytes.toBytes("row_status"),
                                header.getTimestamp(),
                                Bytes.toBytes("D")
                        )
        );
    }

    private void handleAugmentedSchemaEvent(EventHeader header, AugmentedEventData data) throws IOException {
        this.handleAugmentedSchemaEvent(header, data, 1000, 1);
    }

    private void handleAugmentedSchemaEvent(EventHeader header, AugmentedEventData data, int maxVersions, int regions) throws IOException {
        TableName tableName = this.getTableName(header, data);

        try (Admin admin = this.connection.getAdmin()) {
            if (!admin.tableExists(tableName)) {
                HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
                HColumnDescriptor columnDescriptor = new HColumnDescriptor("d");

                columnDescriptor.setMaxVersions(maxVersions);
                tableDescriptor.addFamily(columnDescriptor);

                admin.createTable(tableDescriptor, new RegionSplitter.HexStringSplit().split(regions));
            }
        }

        try (Table table = this.getTable(tableName)) {
            long eventTimestamp = header.getTimestamp();

            String ddl = null;
            String schemaPreChange = null;
            String schemaPostChange = null;
            String createsPreChange = null;
            String createsPostChange = null;

            table.put(
                    new Put(Bytes.toBytes((eventTimestamp > 0)?(Long.toString(eventTimestamp)):("initial-snapshot")))
                            .addColumn(
                                    HBaseEventApplier.CF,
                                    Bytes.toBytes("ddl"),
                                    eventTimestamp,
                                    Bytes.toBytes(ddl)
                            )
                            .addColumn(
                                    HBaseEventApplier.CF,
                                    Bytes.toBytes("schemaPreChange"),
                                    eventTimestamp,
                                    Bytes.toBytes(schemaPreChange)
                            )
                            .addColumn(
                                    HBaseEventApplier.CF,
                                    Bytes.toBytes("schemaPostChange"),
                                    eventTimestamp,
                                    Bytes.toBytes(schemaPostChange)
                            )
                            .addColumn(
                                    HBaseEventApplier.CF,
                                    Bytes.toBytes("createsPreChange"),
                                    eventTimestamp,
                                    Bytes.toBytes(createsPreChange)
                            )
                            .addColumn(
                                    HBaseEventApplier.CF,
                                    Bytes.toBytes("createsPostChange"),
                                    eventTimestamp,
                                    Bytes.toBytes(createsPostChange)
                            )
            );
        }
    }

    private void handleUnknownEvent(EventHeader header, EventData data) {
        HBaseEventApplier.log.log(Level.FINE, "Unknown event type {}", header.getEventType().name());
    }

    private TableName getTableName(EventHeader header, TableNameEventData data) {
        return TableName.valueOf(String.format("%s.%s", this.schema.toLowerCase(), data.getTableName().toLowerCase()));
    }

    private Table getTable(TableName tableName) throws IOException {
        return this.connection.getTable(tableName);
    }

    private byte[] getRowPrimaryKey(AugmentedEventData data, String columnName) throws NoSuchAlgorithmException {
        List<String> rowPrimaryKey = new ArrayList<>();

        for (String primaryKeyColumn : data.getPrimaryKeyColumns()) {
            rowPrimaryKey.add(data.getEventColumns().get(primaryKeyColumn).get(columnName));
        }

        return Bytes.toBytes(this.saltRowPrimaryKey(String.join(";", rowPrimaryKey), rowPrimaryKey.get(0)));
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

        StringBuilder salt = new StringBuilder();

        salt.append(String.format("00%s", byte1hex).substring(byte1hex.length()));
        salt.append(String.format("00%s", byte2hex).substring(byte2hex.length()));
        salt.append(String.format("00%s", byte3hex).substring(byte3hex.length()));
        salt.append(String.format("00%s", byte4hex).substring(byte4hex.length()));

        salt.append(";").append(hbaseRowID);

        return salt.toString();
    }

    @Override
    public void close() throws IOException {
        HBaseEventApplier.connections.keySet().forEach(
                zookeeperQuorum -> {
                    if (HBaseEventApplier.connections.get(zookeeperQuorum) == this.connection) {
                        HBaseEventApplier.connections.remove(zookeeperQuorum);
                    }
                }
        );

        this.connection.close();
    }
}
