package com.booking.replication.schema;

import java.util.HashMap;

/**
 * Created by bosko on 11/4/15.
 */
public class SchemaVersionSnapshot {

    private String schemaVersion_JSONSnaphot;
    private String schemaVersionTables_JSONSnaphot;
    private String schemaVersionCreateStatements_JSONSnapshot;

    public SchemaVersionSnapshot(ActiveSchemaVersion activeSchemaVersion) {
        schemaVersion_JSONSnaphot                  = activeSchemaVersion.toJSON();
        schemaVersionTables_JSONSnaphot            = activeSchemaVersion.schemaTablesToJSON();
        schemaVersionCreateStatements_JSONSnapshot = activeSchemaVersion.schemaCreateStatementsToJSON();
    }

    public String getSchemaVersion_JSONSnaphot() {
        return schemaVersion_JSONSnaphot;
    }

    public String getSchemaVersionTables_JSONSnaphot() {
        return schemaVersionTables_JSONSnaphot;
    }

    public String getSchemaVersionCreateStatements_JSONSnapshot() {
        return schemaVersionCreateStatements_JSONSnapshot;
    }
}
