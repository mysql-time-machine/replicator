package com.booking.replication.augmenter.model.event.format.avro;

import com.booking.replication.augmenter.model.definitions.DDL;
import com.booking.replication.augmenter.model.event.AugmentedEvent;
import com.booking.replication.augmenter.model.event.AugmentedEventData;
import com.booking.replication.augmenter.model.event.AugmentedEventHeader;
import com.booking.replication.augmenter.model.event.AugmentedEventType;
import com.booking.replication.augmenter.model.event.DeleteRowsAugmentedEventData;
import com.booking.replication.augmenter.model.event.EventMetadata;
import com.booking.replication.augmenter.model.event.QueryAugmentedEventData;
import com.booking.replication.augmenter.model.event.QueryAugmentedEventDataType;
import com.booking.replication.augmenter.model.event.RowEventMetadata;
import com.booking.replication.augmenter.model.event.UpdateRowsAugmentedEventData;
import com.booking.replication.augmenter.model.event.WriteRowsAugmentedEventData;
import com.booking.replication.augmenter.model.format.EventDeserializer;
import com.booking.replication.augmenter.model.row.AugmentedRow;
import com.booking.replication.augmenter.model.schema.ColumnSchema;
import com.booking.replication.augmenter.model.schema.FullTableName;
import com.booking.replication.augmenter.model.schema.TableSchema;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class EventDataPresenterAvro {
    private static final Logger LOG = LogManager.getLogger(EventDataPresenterAvro.class);

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final boolean CONVERT_BIN_TO_HEX = true;
    private static final boolean ADD_META_FILEDS = true;

    private Collection<ColumnSchema> columns;
    private Collection<AugmentedRow> rows;
    private FullTableName eventTable;
    private AugmentedEventHeader header;
    private String eventType;
    private String sql;
    private boolean skipRow;

    private boolean isCompatibleSchemaChange;

    public EventDataPresenterAvro(AugmentedEvent event) {
        this.init(event.getHeader(), event.getData());
    }

    private void init(AugmentedEventHeader header, AugmentedEventData eventData) {
        if (eventData instanceof WriteRowsAugmentedEventData) {
            WriteRowsAugmentedEventData data = WriteRowsAugmentedEventData.class.cast(eventData);
            RowEventMetadata metadata = (RowEventMetadata) data.getMetadata();
            this.header = header;
            this.eventTable = metadata.getEventTable();
            this.rows = data.getRows();
            this.columns = metadata.getColumns();
            this.eventType = "insert";
        } else if (eventData instanceof DeleteRowsAugmentedEventData) {
            DeleteRowsAugmentedEventData data = DeleteRowsAugmentedEventData.class.cast(eventData);
            RowEventMetadata metadata = (RowEventMetadata) data.getMetadata();
            this.header = header;
            this.eventTable = metadata.getEventTable();
            this.rows = data.getRows();
            this.columns = metadata.getColumns();
            this.eventType = "delete";
        } else if (eventData instanceof UpdateRowsAugmentedEventData) {
            UpdateRowsAugmentedEventData data = UpdateRowsAugmentedEventData.class.cast(eventData);
            RowEventMetadata metadata = (RowEventMetadata) data.getMetadata();
            this.header = header;
            this.eventTable = metadata.getEventTable();
            this.rows = data.getRows();
            this.columns = metadata.getColumns();
            this.eventType = "update";
        } else if (eventData instanceof QueryAugmentedEventData) {
            QueryAugmentedEventData data = QueryAugmentedEventData.class.cast(eventData);
            EventMetadata metadata = data.getMetadata();
            TableSchema tableSchema = data.getAfter();
            FullTableName eventTable = metadata.getEventTable();
            if (eventTable == null || tableSchema == null) {
                this.skipRow = true;
                return;
            }
            if (data.getQueryType() == QueryAugmentedEventDataType.DDL_TABLE) {
                this.header = header;
                this.eventTable = eventTable;
                this.sql = data.getSQL();
                this.columns = tableSchema.getColumnSchemas();
                this.isCompatibleSchemaChange = data.getIsCompatibleSchemaChange();
                this.eventType = "ddl";
            } else {
                this.skipRow = true;
            }
        } else {
            this.skipRow = true;
        }

    }

    public List<GenericRecord> convertAugmentedEventDataToAvro() throws IOException {

        if (this.skipRow) {
            return new ArrayList<>();
        }

        try {
            Schema avroSchema = createAvroSchema(ADD_META_FILEDS, CONVERT_BIN_TO_HEX, this.eventTable, this.columns);
            if (Objects.equals(this.eventType, "ddl")) {
                final GenericRecord rec = new DDL(
                        this.sql,
                        avroSchema.toString(),
                        this.isCompatibleSchemaChange
                );
                return Collections.singletonList(rec);
            }
            ArrayList<GenericRecord> records = new ArrayList<>();
            for (AugmentedRow row : rows) {
                final GenericRecord rec = new GenericData.Record(avroSchema);

                String key = (row.getEventType() == AugmentedEventType.UPDATE) ? EventDeserializer.Constants.VALUE_AFTER : null ;

                for (String column : row.getValues().keySet()) {
                    rec.put(column, row.getValueAsString(column, key));
                }

                if (ADD_META_FILEDS) {
                    rec.put("__timestamp", header.getTimestamp());
                    int delete = Objects.equals(this.eventType, "delete") ? 1 : 0;
                    rec.put("__is_deleted", delete);
                    long binlogPosition = header.getCheckpoint().getBinlog().getPosition();
                    rec.put("__binlog_position", binlogPosition);
                }

                records.add(rec);
            }
            return records;
        } catch (Exception e) {
            LOG.error("Error while converting data to avro: table: " + this.eventTable + " event header: " + MAPPER.writeValueAsString(this.header), e);
            throw e;
        }
    }

    public static Schema createAvroSchema(boolean addMetaFields, boolean convertBinToHex, FullTableName eventTable, Collection<ColumnSchema> columns) {
        String tableName = eventTable.getName();

        final SchemaBuilder.FieldAssembler<Schema> builder = SchemaBuilder.record(tableName).namespace(eventTable.getDb()).fields();
        /**
         * Some missing Avro types - Decimal, Date types. May need some additional work.
         */
        for (ColumnSchema col : columns) {

            String columnName = col.getName();

            String colType = col.getColumnType();
            if (colType.startsWith("boolean")) {

                // mysql stores it as tinyint
                addIntField(columnName, col.getValueDefault(), builder);
            } else if (colType.startsWith("tinyint")
                    || colType.startsWith("smallint")
                    || colType.startsWith("mediumint")) {
                addIntField(columnName, col.getValueDefault(), builder);
            } else if (colType.startsWith("int") || colType.startsWith("integer")) {
                if (colType.contains("unsigned")) {
                    addLongField(columnName, col.getValueDefault(), builder);
                } else {
                    addIntField(columnName, col.getValueDefault(), builder);
                }
            } else if (colType.startsWith("bigint")) {
                // Check the precision of the BIGINT. Some databases allow arbitrary precision (> 19), but Avro won't handle that.
                // If the precision > 19 (or is negative), use a string for the type, otherwise use a long. The object(s) will be converted
                // to strings as necessary

                if (colType.contains("unsigned")) {
                    addStringField(columnName, col.getValueDefault(), builder);
                } else {
                    addLongField(columnName, col.getValueDefault(), builder);
                }
            } else if (colType.startsWith("float")
                    || colType.startsWith("real")) {
                addFloatField(columnName, col.getValueDefault(), builder);
            } else if (colType.startsWith("double")) {
                addDoubleField(columnName, col.getValueDefault(), builder);
            } else if (colType.startsWith("date")
                    || colType.startsWith("time")
                    || colType.startsWith("timestamp")) {
                addStringField(columnName, col.getValueDefault(), builder);
            } else if (colType.startsWith("binary")
                    || colType.startsWith("varbinary")
                    || colType.startsWith("longvarbinary")
                    || colType.startsWith("array")
                    || colType.startsWith("blob")) {
                if (convertBinToHex) {
                    addStringField(columnName, col.getValueDefault(), builder);
                } else {
                    builder.name(columnName).type().unionOf().nullBuilder().endNull().and().bytesType().endUnion().noDefault();
                }
            } else if (colType.contains("bit")) {
                addStringField(columnName, col.getValueDefault(), builder);
            } else if (colType.startsWith("decimal")
                    || colType.startsWith("numeric")) {
                //todo: get precision and decide data type
                addStringField(columnName, col.getValueDefault(), builder);
            } else {
                addStringField(columnName, col.getValueDefault(), builder);
            }
        }

        if (addMetaFields) {
            addMetaFields(builder);
        }

        return builder.endRecord();
    }

    private static void addIntField(String name, String defaultVal, SchemaBuilder.FieldAssembler<Schema> builder) {
        if (isNullValue(defaultVal)) {
            builder.optionalInt(name);
        } else {
            builder.nullableInt(name, Integer.valueOf(defaultVal));
        }
    }

    private static void addFloatField(String name, String defaultVal, SchemaBuilder.FieldAssembler<Schema> builder) {
        if (isNullValue(defaultVal)) {
            builder.optionalFloat(name);
        } else {
            builder.nullableFloat(name, Float.valueOf(defaultVal));
        }
    }

    private static void addLongField(String name, String defaultVal, SchemaBuilder.FieldAssembler<Schema> builder) {
        if (isNullValue(defaultVal)) {
            builder.optionalLong(name);
        } else {
            builder.nullableLong(name, Long.valueOf(defaultVal));
        }
    }

    private static void addDoubleField(String name, String defaultVal, SchemaBuilder.FieldAssembler<Schema> builder) {
        if (isNullValue(defaultVal)) {
            builder.optionalDouble(name);
        } else {
            builder.nullableDouble(name, Double.valueOf(defaultVal));
        }
    }

    private static void addStringField(String name, String defaultVal, SchemaBuilder.FieldAssembler<Schema> builder) {
        if (isNullValue(defaultVal)) {
            builder.optionalString(name);
        } else {
            builder.nullableString(name, defaultVal);
        }
    }

    private static void addMetaFields(SchemaBuilder.FieldAssembler<Schema> builder) {
        addLongField("__timestamp", "NULL", builder);
        addIntField("__is_deleted", "NULL", builder);
        addLongField("__binlog_position", "NULL", builder);
    }

    private static boolean isNullValue(String val) {
        return val == null || Objects.equals(val.toUpperCase(), "NULL");
    }
}
