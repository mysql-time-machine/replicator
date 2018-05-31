package com.booking.replication.augmenter.active.schema;

import com.booking.replication.augmenter.model.AugmentedEventData;
import com.booking.replication.supplier.model.RawEvent;

import java.util.Map;
import java.util.function.Function;

public class ActiveSchemaDataAugmenter  implements Function<RawEvent, AugmentedEventData> {
    public interface Configuration extends ActiveSchemaAugmenter.Configuration {
        String APPLY_UUID = "augmenter.apply.uuid";
        String APPLY_XID = "augmenter.apply.xid";
        String UUID_FIELD_NAME = "_replicator_uuid";
        String XID_FIELD_NAME = "_replicator_xid";
    }

    public ActiveSchemaDataAugmenter(Map<String, String> configuration) {
    }

    @Override
    public AugmentedEventData apply(RawEvent rawEvent) {
        return null;
    }
}
