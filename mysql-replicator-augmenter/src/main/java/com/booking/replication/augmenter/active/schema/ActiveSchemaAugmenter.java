package com.booking.replication.augmenter.active.schema;

import com.booking.replication.augmenter.Augmenter;
import com.booking.replication.augmenter.model.AugmentedEvent;
import com.booking.replication.augmenter.model.AugmentedEventData;
import com.booking.replication.augmenter.model.AugmentedEventHeader;
import com.booking.replication.supplier.model.RawEvent;
import com.booking.replication.supplier.model.RawEventData;
import com.booking.replication.supplier.model.RawEventHeaderV4;

import java.io.IOException;
import java.util.Map;

public class ActiveSchemaAugmenter implements Augmenter {
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
        RawEventHeaderV4 eventHeader = rawEvent.getHeader();
        RawEventData eventData = rawEvent.getData();

        this.context.updateContext(eventHeader, eventData);

        if (this.context.hasData()) {
            AugmentedEventHeader augmentedEventHeader = this.headerAugmenter.apply(eventHeader, eventData);

            if (augmentedEventHeader == null) {
                return null;
            }

            AugmentedEventData data = this.dataAugmenter.apply(eventHeader, eventData, augmentedEventHeader);

            if (data == null) {
                return null;
            }

            return new AugmentedEvent(augmentedEventHeader, data);
        } else {
            return null;
        }
    }

    @Override
    public void close() throws IOException {
        this.context.close();
    }
}
