package com.booking.replication.augmenter;

import com.booking.replication.schema.SchemaVersionSnapshot;
import com.booking.replication.util.JSONBuilder;

/**
 * This class has all extra info needed for
 * sorting DDL events.
 */
public class AugmentedSchemaChangeEvent {

    private String database_name;
    private String schemaTransitionDDLStatement;

    private SchemaVersionSnapshot preTransitionSchemaSnapshot;
    private SchemaVersionSnapshot postTransitionSchemaSnapshot;

    public AugmentedSchemaChangeEvent(
            SchemaVersionSnapshot snapshotBefore,
            String ddlSQL,
            SchemaVersionSnapshot snapshotAfter,
            String databaseName) {

        preTransitionSchemaSnapshot  = snapshotBefore;
        schemaTransitionDDLStatement = ddlSQL;
        postTransitionSchemaSnapshot = snapshotAfter;
        database_name = databaseName;
    }

    public String toJSON() {
        return JSONBuilder.schemaChangeEventToJSON(this);
    }
    public String getDatabase_name() {
        return database_name;
    }

    public void setDatabase_name(String database_name) {
        this.database_name = database_name;
    }

    public String getSchemaTransitionDDLStatement() {
        return schemaTransitionDDLStatement;
    }

    public void setSchemaTransitionDDLStatement(String schemaTransitionDDLStatement) {
        this.schemaTransitionDDLStatement = schemaTransitionDDLStatement;
    }

    public SchemaVersionSnapshot getPreTransitionSchemaSnapshot() {
        return preTransitionSchemaSnapshot;
    }

    public void setPreTransitionSchemaSnapshot(SchemaVersionSnapshot preTransitionSchemaSnapshot) {
        this.preTransitionSchemaSnapshot = preTransitionSchemaSnapshot;
    }

    public SchemaVersionSnapshot getPostTransitionSchemaSnapshot() {
        return postTransitionSchemaSnapshot;
    }

    public void setPostTransitionSchemaSnapshot(SchemaVersionSnapshot postTransitionSchemaSnapshot) {
        this.postTransitionSchemaSnapshot = postTransitionSchemaSnapshot;
    }
}
