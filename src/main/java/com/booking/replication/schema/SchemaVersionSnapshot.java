package com.booking.replication.schema;

/**
 * Created by bosko on 11/4/15.
 */
public class SchemaVersionSnapshot {

    private String schemaVersionJsonSnaphot;
    private String schemaVersionTablesJsonSnaphot;
    private String schemaVersionCreateStatementsJsonSnapshot;

    public SchemaVersionSnapshot(ActiveSchemaVersion activeSchemaVersion) {
        schemaVersionJsonSnaphot = activeSchemaVersion.toJson();
        schemaVersionTablesJsonSnaphot = activeSchemaVersion.schemaTablesToJson();
        schemaVersionCreateStatementsJsonSnapshot = activeSchemaVersion.schemaCreateStatementsToJson();
    }

    public String getSchemaVersionJsonSnaphot() {
        return schemaVersionJsonSnaphot;
    }

    public String getSchemaVersionTablesJsonSnaphot() {
        return schemaVersionTablesJsonSnaphot;
    }

    public String getSchemaVersionCreateStatementsJsonSnapshot() {
        return schemaVersionCreateStatementsJsonSnapshot;
    }
}
