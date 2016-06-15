package com.booking.replication.schema;

/**
 * Created by bosko on 11/4/15.
 */
public class SchemaVersionSnapshot {

    private String schemaVersionJSONSnaphot;
    private String schemaVersionTablesJSONSnaphot;
    private String schemaVersionCreateStatementsJSONSnapshot;

    public SchemaVersionSnapshot(ActiveSchemaVersion activeSchemaVersion) {
        schemaVersionJSONSnaphot = activeSchemaVersion.toJSON();
        schemaVersionTablesJSONSnaphot = activeSchemaVersion.schemaTablesToJSON();
        schemaVersionCreateStatementsJSONSnapshot = activeSchemaVersion.schemaCreateStatementsToJSON();
    }

    public String getSchemaVersionJSONSnaphot() {
        return schemaVersionJSONSnaphot;
    }

    public String getSchemaVersionTablesJSONSnaphot() {
        return schemaVersionTablesJSONSnaphot;
    }

    public String getSchemaVersionCreateStatementsJSONSnapshot() {
        return schemaVersionCreateStatementsJSONSnapshot;
    }
}
