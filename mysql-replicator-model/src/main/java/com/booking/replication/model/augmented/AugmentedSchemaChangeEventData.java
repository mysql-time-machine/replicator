package com.booking.replication.model.augmented;

import com.booking.replication.model.augmented.active.schema.SchemaVersionSnapshot;

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
