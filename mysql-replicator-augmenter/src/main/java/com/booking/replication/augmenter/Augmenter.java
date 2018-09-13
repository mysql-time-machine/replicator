package com.booking.replication.augmenter;

import com.booking.replication.augmenter.model.event.AugmentedEvent;
import com.booking.replication.augmenter.model.event.AugmentedEventType;
import com.booking.replication.augmenter.model.schema.ColumnSchema;
import com.booking.replication.augmenter.model.event.AugmentedEventData;
import com.booking.replication.augmenter.model.event.AugmentedEventHeader;
import com.booking.replication.augmenter.model.schema.SchemaSnapshot;
import com.booking.replication.commons.checkpoint.ForceRewindException;
import com.booking.replication.supplier.model.RawEvent;
import com.booking.replication.supplier.model.RawEventData;
import com.booking.replication.supplier.model.RawEventHeaderV4;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class Augmenter implements Function<RawEvent, Collection<AugmentedEvent>>, Closeable {
    public enum SchemaType {
        NONE {
            @Override
            protected SchemaManager newInstance(Map<String, Object> configuration)
            {
                return new SchemaManager() {
                    @Override
                    public boolean execute(String tableName, String query) {
                        return false;
                    }

                    @Override
                    public List<ColumnSchema> listColumns(String tableName) {
                        return null;
                    }

                    @Override
                    public String getCreateTable(String tableName) {
                        return null;
                    }

                    @Override
                    public void close() throws IOException {
                    }
                };
            }
        },
        ACTIVE {
            @Override
            protected SchemaManager newInstance(Map<String, Object> configuration) {
                return new ActiveSchemaManager(configuration);
            }
        };

        protected abstract SchemaManager newInstance(Map<String, Object> configuration);
    }

    public interface Configuration {
        String SCHEMA_TYPE = "augmenter.schema.type";
    }

    private final AugmenterContext context;
    private final HeaderAugmenter headerAugmenter;
    private final DataAugmenter dataAugmenter;

    private Augmenter(SchemaManager schemaManager, Map<String, Object> configuration) {
        this.context = new AugmenterContext(schemaManager, configuration);
        this.headerAugmenter = new HeaderAugmenter(this.context);
        this.dataAugmenter = new DataAugmenter(this.context);
    }

    @Override
    public Collection<AugmentedEvent> apply(RawEvent rawEvent) {

        try {

            RawEventHeaderV4 eventHeader = rawEvent.getHeader();
            RawEventData eventData = rawEvent.getData();

            this.context.updateContext(eventHeader, eventData);

            if (this.context.shouldProcess()) {

                if (this.context.getTransaction().markedForCommit()) {

                    // marked for commit
                    if (this.context.getTransaction().sizeLimitExceeded()) {

                        // size limit exceeded, drop current transaction & rewind
                        this.context.getTransaction().getAndClear();
                        this.context.getTransaction().rewind();

                        throw new ForceRewindException("transaction size limit exceeded");

                    } else {
                        // transaction size ok, extract & return augmented events
                        Collection<AugmentedEvent> augmentedEvents = this.context.getTransaction().getAndClear();

                        if (augmentedEvents.size() > 0) {
                            return augmentedEvents;
                        } else {
                            return null;
                        }
                    }

                } else { // commit not reached

                    // Augment the event
                    AugmentedEventHeader augmentedEventHeader = this.headerAugmenter.apply(eventHeader, eventData);

                    if (augmentedEventHeader == null) {
                        return null;
                    }

                    AugmentedEventData augmentedEventData = this.dataAugmenter.apply(eventHeader, eventData);

                    if (augmentedEventData == null) {
                        return null;
                    }

                    AugmentedEvent augmentedEvent = new AugmentedEvent(augmentedEventHeader, augmentedEventData);

                    // Optional payload for DDL event:
                    //      - if current event is DDL, there will be an updated schema snapshot
                    //        in the context object and isAtDdl will be true
                    //      - for HBase, we inject this extra information to the augmented event because
                    //        it is needed by the HBaseApplier

                    if (this.context.isAtDdl()) {
                        if (augmentedEvent.getHeader().getEventType() == AugmentedEventType.QUERY) {
                            SchemaSnapshot schemaSnapshot = this.context.getSchemaSnapshot();
                            augmentedEvent.setOptionalPayload(schemaSnapshot);
                        } else {
                            throw new RuntimeException("Error in logic");
                        }
                    }

                    if (this.context.getTransaction().started()) {

                        if (this.context.getTransaction().resuming() && this.context.getTransaction().sizeLimitExceeded()) {

                            Collection<AugmentedEvent> augmentedEvents = this.context.getTransaction().getAndClear();

                            this.context.getTransaction().add(augmentedEvent);

                            return augmentedEvents;
                        } else {
                            this.context.getTransaction().add(augmentedEvent);

                            return null;
                        }
                    } else {
                        return Collections.singletonList(augmentedEvent);
                    }
                }
            } else {
                return null;
            }
        } finally {
            this.context.updatePosition();
        }
    }

    @Override
    public void close() throws IOException {
        this.context.close();
    }

    public static Augmenter build(Map<String, Object> configuration) {
        return new Augmenter(
                Augmenter.SchemaType.valueOf(
                    configuration.getOrDefault(Configuration.SCHEMA_TYPE, SchemaType.NONE.name()).toString()
                ).newInstance(configuration),
                configuration
        );
    }
}