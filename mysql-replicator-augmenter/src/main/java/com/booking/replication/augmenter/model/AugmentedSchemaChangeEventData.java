package com.booking.replication.augmenter.model;

import com.booking.replication.augmenter.active.schema.augmented.active.schema.SchemaVersionSnapshot;

import java.util.HashMap;

/**
 * Created by smalviya on 2/7/18.
 */
public interface AugmentedSchemaChangeEventData {
    public HashMap<String, String> getSchemaTransitionSequence();

    public SchemaVersionSnapshot getPreTransitionSchemaSnapshot();

    public SchemaVersionSnapshot getPostTransitionSchemaSnapshot();

    public long getSchemaChangeEventTimestamp();
}
