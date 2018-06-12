package com.booking.replication.augmenter.active.schema;

import com.booking.replication.augmenter.Augmenter;
import com.booking.replication.augmenter.model.AugmentedEvent;
import com.booking.replication.augmenter.model.AugmentedEventData;
import com.booking.replication.augmenter.model.AugmentedEventHeader;
import com.booking.replication.augmenter.model.AugmentedEventType;
import com.booking.replication.augmenter.model.TransactionAugmentedEventData;
import com.booking.replication.commons.checkpoint.ForceRewindException;
import com.booking.replication.supplier.model.RawEvent;
import com.booking.replication.supplier.model.RawEventData;
import com.booking.replication.supplier.model.RawEventHeaderV4;

import java.io.IOException;
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
    public AugmentedEvent apply(RawEvent rawEvent) {
        RawEventHeaderV4 eventHeader = rawEvent.getHeader();
        RawEventData eventData = rawEvent.getData();

        this.context.updateContext(eventHeader, eventData);

        if (this.context.hasData()) {
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
                    TransactionAugmentedEventData transactionAugmentedEventData = this.context.getTransaction().clean();

                    if (transactionAugmentedEventData.getEventList().size() > 0) {
                        return this.getTransactionAugmentedEvent(transactionAugmentedEventData);
                    } else {
                        return null;
                    }
                }
            } else if (this.context.getTransaction().started()) {
                if (this.context.getTransaction().resuming() && this.context.getTransaction().sizeLimitExceeded()) {
                    TransactionAugmentedEventData transactionAugmentedEventData = this.context.getTransaction().clean();

                    this.context.getTransaction().add(new AugmentedEvent(augmentedEventHeader, augmentedEventData));

                    return this.getTransactionAugmentedEvent(transactionAugmentedEventData);
                } else {
                    this.context.getTransaction().add(new AugmentedEvent(augmentedEventHeader, augmentedEventData));

                    return null;
                }
            } else {
                return new AugmentedEvent(augmentedEventHeader, augmentedEventData);
            }
        } else {
            return null;
        }
    }

    private AugmentedEvent getTransactionAugmentedEvent(TransactionAugmentedEventData transactionAugmentedEventData) {
        return new AugmentedEvent(
                new AugmentedEventHeader(
                        this.context.getTransaction().getTimestamp(),
                        transactionAugmentedEventData.getEventList().get(transactionAugmentedEventData.getEventList().size() - 1).getHeader().getCheckpoint(),
                        AugmentedEventType.TRANSACTION
                ),
                transactionAugmentedEventData
        );
    }

    @Override
    public void close() throws IOException {
        this.context.close();
    }
}
