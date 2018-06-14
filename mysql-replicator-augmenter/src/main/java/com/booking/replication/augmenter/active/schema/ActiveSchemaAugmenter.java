package com.booking.replication.augmenter.active.schema;

import com.booking.replication.augmenter.Augmenter;
import com.booking.replication.augmenter.model.AugmentedEvent;
import com.booking.replication.augmenter.model.AugmentedEventData;
import com.booking.replication.augmenter.model.AugmentedEventHeader;
import com.booking.replication.commons.checkpoint.ForceRewindException;
import com.booking.replication.supplier.model.RawEvent;
import com.booking.replication.supplier.model.RawEventData;
import com.booking.replication.supplier.model.RawEventHeaderV4;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class ActiveSchemaAugmenter implements Augmenter {
    private final ActiveSchemaContext context;
    private final ActiveSchemaHeaderAugmenter headerAugmenter;
    private final ActiveSchemaDataAugmenter dataAugmenter;

    public ActiveSchemaAugmenter(Map<String, Object> configuration) {
        this.context = new ActiveSchemaContext(configuration);
        this.headerAugmenter = new ActiveSchemaHeaderAugmenter(this.context);
        this.dataAugmenter = new ActiveSchemaDataAugmenter(this.context);
    }

    @Override
    public List<AugmentedEvent> apply(RawEvent rawEvent) {
        RawEventHeaderV4 eventHeader = rawEvent.getHeader();
        RawEventData eventData = rawEvent.getData();

        this.context.updateContext(eventHeader, eventData);

        if (this.context.process()) {
            AugmentedEventHeader augmentedEventHeader = this.headerAugmenter.apply(eventHeader, eventData);

            if (augmentedEventHeader == null) {
                return null;
            }

            AugmentedEventData augmentedEventData = this.dataAugmenter.apply(eventHeader, eventData);

            if (augmentedEventData == null) {
                return null;
            }

            if (this.context.getTransaction().committed()) {
                if (this.context.getTransaction().sizeLimitExceeded()) {
                    this.context.getTransaction().clean();
                    this.context.getTransaction().rewind();

                    throw new ForceRewindException("transaction size limit exceeded");
                } else {
                    List<AugmentedEvent> augmentedEventList = this.context.getTransaction().clean();

                    if (augmentedEventList.size() > 0) {
                        return augmentedEventList;
                    } else {
                        return null;
                    }
                }
            } else if (this.context.getTransaction().started()) {
                if (this.context.getTransaction().resuming() && this.context.getTransaction().sizeLimitExceeded()) {
                    List<AugmentedEvent> augmentedEventList = this.context.getTransaction().clean();

                    this.context.getTransaction().add(new AugmentedEvent(augmentedEventHeader, augmentedEventData));

                    return augmentedEventList;
                } else {
                    this.context.getTransaction().add(new AugmentedEvent(augmentedEventHeader, augmentedEventData));

                    return null;
                }
            } else {
                return Collections.singletonList(new AugmentedEvent(augmentedEventHeader, augmentedEventData));
            }
        } else {
            return null;
        }
    }

    @Override
    public void close() throws IOException {
        this.context.close();
    }
}
