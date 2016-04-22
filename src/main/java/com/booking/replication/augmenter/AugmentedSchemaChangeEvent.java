package com.booking.replication.augmenter;

import com.booking.replication.schema.SchemaVersionSnapshot;
import com.booking.replication.util.JSONBuilder;

import java.util.HashMap;

/**
 * This class has all extra info needed for
 * sorting DDL events.
 */
public class AugmentedSchemaChangeEvent {

    private String database_name;
    private HashMap<String,String> schemaTransitionSequence;

    private SchemaVersionSnapshot preTransitionSchemaSnapshot;
    private SchemaVersionSnapshot postTransitionSchemaSnapshot;

    public AugmentedSchemaChangeEvent(
            SchemaVersionSnapshot snapshotBefore,
            HashMap<String,String> transitionSequence,
            SchemaVersionSnapshot snapshotAfter,
            String databaseName) {

        preTransitionSchemaSnapshot  = snapshotBefore;
        schemaTransitionSequence = transitionSequence;
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

    public HashMap<String,String> getSchemaTransitionSequence() {
        return schemaTransitionSequence;
    }

    public void setSchemaTransitionSequence(HashMap<String,String> schemaTransitionSequence) {
        this.schemaTransitionSequence = schemaTransitionSequence;
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
