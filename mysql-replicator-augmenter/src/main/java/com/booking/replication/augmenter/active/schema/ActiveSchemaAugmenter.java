package com.booking.replication.augmenter.active.schema;

import com.booking.replication.augmenter.Augmenter;
import com.booking.replication.augmenter.model.AugmentedEvent;
import com.booking.replication.augmenter.model.AugmentedEventData;
import com.booking.replication.augmenter.model.AugmentedEventHeader;
import com.booking.replication.supplier.model.RawEvent;

import java.util.Map;

public class ActiveSchemaAugmenter implements Augmenter {
    interface Configuration {
        String ACTIVE_SCHEMA = "augmenter.active.schema";
    }

    private final ActiveSchemaContext context;
    private final ActiveSchemaHeaderAugmenter headerAugmenter;
    private final ActiveSchemaDataAugmenter dataAugmenter;

    public ActiveSchemaAugmenter(Map<String, String> configuration) {
        this.context = new ActiveSchemaContext(configuration);
        this.headerAugmenter = new ActiveSchemaHeaderAugmenter(this.context);
        this.dataAugmenter = new ActiveSchemaDataAugmenter(this.context);
    }

    @Override
    public AugmentedEvent apply(RawEvent rawEvent) {
        AugmentedEventHeader header = this.headerAugmenter.apply(rawEvent);

        if (header == null) {
            return null;
        }

        AugmentedEventData data = this.dataAugmenter.apply(rawEvent);

        if (data == null) {
            return null;
        }

        return new AugmentedEvent(header, data);
    }
}
