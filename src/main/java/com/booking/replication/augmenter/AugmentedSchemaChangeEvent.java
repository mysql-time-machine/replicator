package com.booking.replication.augmenter;

import com.booking.replication.schema.SchemaVersionSnapshot;
import com.booking.replication.util.JSONBuilder;

import java.util.HashMap;

/**
 * This class has all extra info needed for
 * sorting DDL events.
 */
public class AugmentedSchemaChangeEvent {

    private final long             schemaChangeEventTimestamp;

    private SchemaVersionSnapshot  preTransitionSchemaSnapshot;

    private HashMap<String,String> schemaTransitionSequence;

    private SchemaVersionSnapshot  postTransitionSchemaSnapshot;

    public AugmentedSchemaChangeEvent(
            SchemaVersionSnapshot snapshotBefore,
            HashMap<String,String> transitionSequence,
            SchemaVersionSnapshot snapshotAfter,
            long ddlTimestamp) {

        preTransitionSchemaSnapshot  = snapshotBefore;
        schemaTransitionSequence = transitionSequence;
        postTransitionSchemaSnapshot = snapshotAfter;
        schemaChangeEventTimestamp   = ddlTimestamp;
    }

    public String toJSON() {
        return JSONBuilder.schemaChangeEventToJSON(this);
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

    public long getSchemaChangeEventTimestamp() {
        return schemaChangeEventTimestamp;
    }
}
