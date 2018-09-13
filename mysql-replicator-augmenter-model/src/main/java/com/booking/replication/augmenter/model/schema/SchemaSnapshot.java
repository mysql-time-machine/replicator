package com.booking.replication.augmenter.model.schema;

public class SchemaSnapshot {

    private final SchemaTransitionSequence schemaTransitionSequence;
    private final SchemaAtPositionCache schemaBefore;
    private final SchemaAtPositionCache schemaAfter;

    public SchemaSnapshot(
            SchemaTransitionSequence schemaTransitionSequence,
            SchemaAtPositionCache schemaBefore,
            SchemaAtPositionCache schemaAfter) {

        this.schemaTransitionSequence = schemaTransitionSequence;
        this.schemaBefore = schemaBefore;
        this.schemaAfter = schemaAfter;
    }

    public SchemaTransitionSequence getSchemaTransitionSequence() {
        return schemaTransitionSequence;
    }

    public SchemaAtPositionCache getSchemaBefore() {
        return schemaBefore;
    }

    public SchemaAtPositionCache getSchemaAfter() {
        return schemaAfter;
    }
}
