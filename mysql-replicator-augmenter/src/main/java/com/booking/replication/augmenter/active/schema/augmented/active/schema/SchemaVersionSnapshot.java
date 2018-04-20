package com.booking.replication.augmenter.active.schema.augmented.active.schema;

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
