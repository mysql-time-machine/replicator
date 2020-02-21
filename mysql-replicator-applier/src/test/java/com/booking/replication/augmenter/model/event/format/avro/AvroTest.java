package com.booking.replication.augmenter.model.event.format.avro;

import com.booking.replication.applier.message.format.avro.AvroUtils;
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
import org.junit.Test;

import java.io.IOException;
import java.util.*;

import static org.junit.Assert.*;

public class AvroTest {
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

    @Test
    public void testAugmentedEventEncoding() throws IOException {

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

        AugmentedEvent augmentedEvent = new AugmentedEvent(augmentedEventHeader, augmentedEventData);

        AvroManager dataPresenter = new AvroManager(augmentedEvent);

        List<GenericRecord> records =  dataPresenter.convertAugmentedEventDataToAvro();

        records.stream().forEach(rec -> {

            Schema schema = rec.getSchema();
            System.out.println("schema => " + schema);
            try {
                byte[] blob = AvroUtils.serializeAvroGenericRecord(rec);
                GenericRecord back = AvroUtils.deserializeAvroBlob(blob, schema);
                System.out.println(back.get("col1") + "," + back.get("col2") + back.get("col3"));
            } catch (IOException e) {
                e.printStackTrace();
            }
        });


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