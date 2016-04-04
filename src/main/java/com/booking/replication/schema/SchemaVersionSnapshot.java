package com.booking.replication.schema;

import java.util.HashMap;

/**
 * Created by bosko on 11/4/15.
 */
public class SchemaVersionSnapshot {

    private String schemaVersionJSONSnaphot;

    public SchemaVersionSnapshot(ActiveSchemaVersion activeSchemaVersion) {
        schemaVersionJSONSnaphot = activeSchemaVersion.toJSON();
    }

    public String getSchemaVersionJSONSnaphot() {
        return schemaVersionJSONSnaphot;
    }

    public void setSchemaVersionJSONSnaphot(String schemaVersionJSONSnaphot) {
        this.schemaVersionJSONSnaphot = schemaVersionJSONSnaphot;
    }
}
