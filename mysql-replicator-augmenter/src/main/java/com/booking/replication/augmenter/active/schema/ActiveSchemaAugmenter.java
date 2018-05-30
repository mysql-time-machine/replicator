package com.booking.replication.augmenter.active.schema;

import com.booking.replication.augmenter.Augmenter;
import com.booking.replication.augmenter.model.AugmentedEvent;
import com.booking.replication.supplier.model.RawEvent;

import java.util.Map;

public class ActiveSchemaAugmenter implements Augmenter {
    interface Configuration extends Augmenter.Configuration {
        String ACTIVE_SCHEMA       = "augmenter.active.schema";
    }

    private final ActiveSchemaHeaderAugmenter headerAugmenter;
    private final ActiveSchemaDataAugmenter dataAugmenter;

    public ActiveSchemaAugmenter(Map<String, String> configuration) {
        this.headerAugmenter = new ActiveSchemaHeaderAugmenter(configuration);
        this.dataAugmenter = new ActiveSchemaDataAugmenter(configuration);
    }

    @Override
    public AugmentedEvent apply(RawEvent rawEvent) {
        return new AugmentedEvent(
                this.headerAugmenter.apply(rawEvent),
                this.dataAugmenter.apply(rawEvent)
        );
    }
}
