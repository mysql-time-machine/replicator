package com.booking.replication.augmenter.model.event.format.avro;

import com.booking.replication.augmenter.model.event.AugmentedEvent;
import com.booking.replication.augmenter.model.schema.ColumnSchema;
import com.booking.replication.augmenter.model.schema.FullTableName;
import org.apache.avro.Schema;
import org.junit.Test;

import java.util.ArrayList;

import static org.junit.Assert.*;

public class EventDataPresenterAvroTest {
    @Test
    public void testDefaultValuesInGeneratedAvroSchema() throws Exception {
        EventDataPresenterAvro dataPresenter = new EventDataPresenterAvro(new AugmentedEvent());
        ArrayList<ColumnSchema> columns = new ArrayList<>();
        columns.add(new ColumnSchema(
                "col1",
                "int(11)",
                "NULL",
                true,
                "",
                null,
                ""
        ));
        columns.add(new ColumnSchema(
                "col2",
                "int(11)",
                "NULL",
                true,
                "",
                "10",
                ""
        ));
        columns.add(new ColumnSchema(
                "col3",
                "enum('boolean','integer','string','date','datetime','boolarray','intarray','stringarray','datearray','enum')",
                "NULL",
                true,
                "",
                "string",
                ""
        ));
        Schema avroSchema = dataPresenter.createAvroSchema(false, true,
                new FullTableName("db", "table"),
                columns);
        String expected = "{\"type\":\"record\",\"name\":\"table\",\"namespace\":\"db\",\"fields\":[{\"name\":\"col1\",\"type\":[\"null\",\"int\"],\"default\":null},{\"name\":\"col2\",\"type\":[\"int\",\"null\"],\"default\":10},{\"name\":\"col3\",\"type\":[\"string\",\"null\"],\"default\":\"string\"}]}";
        assertEquals(expected, avroSchema.toString());

    }

}