package com.booking.replication.augmenter.model.event.format.avro;

import com.booking.replication.applier.message.format.avro.AvroUtils;
import com.booking.replication.applier.message.format.avro.SerializedEvent;
import com.booking.replication.augmenter.model.event.*;
import com.booking.replication.augmenter.model.row.AugmentedRow;
import com.booking.replication.augmenter.model.schema.ColumnSchema;
import com.booking.replication.augmenter.model.schema.DataType;
import com.booking.replication.augmenter.model.schema.FullTableName;
import com.booking.replication.commons.checkpoint.Binlog;
import com.booking.replication.commons.checkpoint.Checkpoint;
import com.google.common.collect.ImmutableMap;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.testcontainers.shaded.com.google.common.collect.Multiset;

import java.io.IOException;
import java.util.*;

import static org.junit.Assert.*;

public class AvroTest {

    // id => schema (in this dummy registry versions are covered by different ids)
    private static Map<Integer, Schema> schemaRegistry;

    @BeforeClass
    public static void setupDummySchemaRegistry() {
        schemaRegistry = new HashMap<>();
    }

    @Test
    public void testDefaultValuesInGeneratedAvroSchema() throws Exception {

        AvroManager dataPresenter = new AvroManager(new AugmentedEvent());

        List<ColumnSchema> columns = getColumnSchemaList();

        Schema avroSchema = dataPresenter.createAvroSchema(
                false,
                true,
                new FullTableName("db", "table"),
                columns
        );

        String expected = "{\"type\":\"record\",\"name\":\"table\",\"namespace\":\"db\",\"fields\":[{\"name\":\"col1\",\"type\":[\"null\",\"int\"],\"default\":null},{\"name\":\"col2\",\"type\":[\"int\",\"null\"],\"default\":10},{\"name\":\"col3\",\"type\":[\"string\",\"null\"],\"default\":\"string\"}]}";

        assertEquals(expected, avroSchema.toString());

    }

    private Integer registerAndGetSchemaId(Schema schema) {
        // TODO:
        schemaRegistry.put(1, schema);
        return 1;
    }

    @Test
    public void testAugmentedEventEncodeDecode() throws IOException {

        AugmentedEvent augmentedEvent = getAugmentedEvent();

        AvroManager avroManager = new AvroManager(augmentedEvent);
        List<GenericRecord> records =  avroManager.convertAugmentedEventDataToAvro();

        records.stream().forEach(rec -> {


            Schema schema = rec.getSchema();
            Integer schemaId = registerAndGetSchemaId(schema);

            System.out.println("schema => " + schema);

            String expected = rec.get("col1") + ";" + rec.get("col2") + ";" + rec.get("col3");

            try {

                System.out.println("in => " + rec.get("col1") + ";" + rec.get("col2") + ";" + rec.get("col3"));

                byte[] blob = AvroUtils.serializeAvroGenericRecord(rec, schemaId);

                SerializedEvent serializedEvent = AvroUtils.extractSerializedEvent(blob);

                Schema retrievedSchema = schemaRegistry.get(schemaId);

                GenericRecord back = AvroUtils.deserializeAvroBlob(serializedEvent.getEventDataAvroBlob(), retrievedSchema);

                System.out.println("out => " + back.get("col1") + ";" + back.get("col2") + ";" + back.get("col3"));

                String received = back.get("col1") + ";" + back.get("col2") + ";" + back.get("col3");

                assertEquals("Binary encoding/decoding test", expected, received );

            } catch (IOException e) {
                e.printStackTrace();
            }
        });


    }

    @NotNull
    private AugmentedEvent getAugmentedEvent() {
        AugmentedEventHeader augmentedEventHeader = new AugmentedEventHeader(
                1582286646,
                new Checkpoint(
                        new Binlog("binlog.000001", 4),
                        "303f4b46-54a7-11ea-9033-0242ac110004:1-6"
                ),
                AugmentedEventType.INSERT,
                "dbName",
                "tableName"
        );

        AugmentedEventData augmentedEventData = new WriteRowsAugmentedEventData(
                AugmentedEventType.INSERT,
                new FullTableName("dbName", "tableName"),
                Arrays.asList(true, true, true), // included columns
                getColumnSchemaList(),
                getAugmentedEventRows()
        );

        return new AugmentedEvent(augmentedEventHeader, augmentedEventData);
    }


    private List<ColumnSchema> getColumnSchemaList() {
        ArrayList<ColumnSchema> columns = new ArrayList<>();

        columns.add(
                new ColumnSchema("col1", DataType.INT, "int(11)", true, "", "")
        );
        columns.add(
                new ColumnSchema("col2", DataType.INT, "int(11)", true, "", "")
                .setDefaultValue("10")
        );
        columns.add(
                new ColumnSchema(
                "col3", DataType.ENUM,
                "enum('boolean','integer','string','date','datetime','boolarray','intarray','stringarray','datearray','enum')", true, "", "")
                .setDefaultValue("string")
        );

        return columns;
    }

    private Collection<AugmentedRow> getAugmentedEventRows() {

        Collection<AugmentedRow> augmentedRows = new ArrayList<>();
        List<ColumnSchema> columns = getColumnSchemaList();
        Map<String, Object> deserialisedCellValues = ImmutableMap.of("col1", 1, "col2", 2, "col3", "test");

        String schemaName = "testDb";
        String tableName = "testTable";
        Long commitTimestamp = 1582288765000L;

        String transactionUUID = "303f4b46-54a7-11ea-9033-0242ac110004:6";
        List<String> primaryKeyColumns = Arrays.asList("col1");

        AugmentedRow augmentedRow = new AugmentedRow(
                AugmentedEventType.INSERT,
                schemaName,
                tableName,
                commitTimestamp,
                transactionUUID,
                0L,
                primaryKeyColumns,
                deserialisedCellValues
        );

        augmentedRows.add(augmentedRow);

        return augmentedRows;
    }

}