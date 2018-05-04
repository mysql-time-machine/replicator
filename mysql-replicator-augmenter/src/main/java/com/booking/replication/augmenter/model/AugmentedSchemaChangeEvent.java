package com.booking.replication.augmenter.model;


import com.booking.replication.augmenter.util.JsonBuilder;
import com.booking.replication.augmenter.model.AugmentedSchemaChangeEventData;
import com.booking.replication.augmenter.active.schema.augmented.active.schema.SchemaVersionSnapshot;

import java.util.HashMap;

/**
 * This class has all extra info needed for
 * sorting DDL events.
 */
public class AugmentedSchemaChangeEvent implements AugmentedSchemaChangeEventData {

    private final long schemaChangeEventTimestamp;

    private SchemaVersionSnapshot preTransitionSchemaSnapshot;

    private HashMap<String, String> schemaTransitionSequence;

    private SchemaVersionSnapshot postTransitionSchemaSnapshot;

    /**
     * DDL event.
     *
     * @param snapshotBefore     Schema version snapshot
     * @param transitionSequence Schema transition sequence
     * @param snapshotAfter      Schema version snapshot
     * @param ddlTimestamp       Timestamp
     */
    public AugmentedSchemaChangeEvent(
            SchemaVersionSnapshot snapshotBefore,
            HashMap<String, String> transitionSequence,
            SchemaVersionSnapshot snapshotAfter,
            long ddlTimestamp) {

        preTransitionSchemaSnapshot = snapshotBefore;
        schemaTransitionSequence = transitionSequence;
        postTransitionSchemaSnapshot = snapshotAfter;
        schemaChangeEventTimestamp = ddlTimestamp;
    }

    public String toJson() {
        return JsonBuilder.schemaChangeEventToJson(this);
    }

    public HashMap<String, String> getSchemaTransitionSequence() {
        return schemaTransitionSequence;
    }

    public void setSchemaTransitionSequence(HashMap<String, String> schemaTransitionSequence) {
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
