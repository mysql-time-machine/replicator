package com.booking.replication.augmenter;

import com.booking.replication.augmenter.model.AugmentedEvent;
import com.booking.replication.augmenter.model.AugmentedEventColumn;
import com.booking.replication.augmenter.model.AugmentedEventData;
import com.booking.replication.augmenter.model.AugmentedEventHeader;
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
            protected Schema newInstance(Map<String, Object> configuration)
            {
                return new Schema() {
                    @Override
                    public boolean execute(String tableName, String query) {
                        return false;
                    }

                    @Override
                    public List<AugmentedEventColumn> listColumns(String tableName) {
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
            protected Schema newInstance(Map<String, Object> configuration) {
                return new ActiveSchema(configuration);
            }
        };

        protected abstract Schema newInstance(Map<String, Object> configuration);
    }

    public interface Configuration {
        String SCHEMA_TYPE = "augmenter.schema.type";
    }

    private final AugmenterContext context;
    private final HeaderAugmenter headerAugmenter;
    private final DataAugmenter dataAugmenter;

    private Augmenter(Schema schema, Map<String, Object> configuration) {
        this.context = new AugmenterContext(schema, configuration);
        this.headerAugmenter = new HeaderAugmenter(this.context);
        this.dataAugmenter = new DataAugmenter(this.context);
    }

    @Override
    public Collection<AugmentedEvent> apply(RawEvent rawEvent) {
        try {
            RawEventHeaderV4 eventHeader = rawEvent.getHeader();
            RawEventData eventData = rawEvent.getData();

            this.context.updateContext(eventHeader, eventData);

            if (this.context.process()) {
                if (this.context.getTransaction().committed()) {
                    if (this.context.getTransaction().sizeLimitExceeded()) {
                        this.context.getTransaction().clean();
                        this.context.getTransaction().rewind();

                        throw new ForceRewindException("transaction size limit exceeded");
                    } else {
                        Collection<AugmentedEvent> augmentedEvents = this.context.getTransaction().clean();

                        if (augmentedEvents.size() > 0) {
                            return augmentedEvents;
                        } else {
                            return null;
                        }
                    }
                } else {
                    AugmentedEventHeader augmentedEventHeader = this.headerAugmenter.apply(eventHeader, eventData);

                    if (augmentedEventHeader == null) {
                        return null;
                    }

                    AugmentedEventData augmentedEventData = this.dataAugmenter.apply(eventHeader, eventData);

                    if (augmentedEventData == null) {
                        return null;
                    }

                    AugmentedEvent augmentedEvent = new AugmentedEvent(augmentedEventHeader, augmentedEventData);

                    if (this.context.getTransaction().started()) {
                        if (this.context.getTransaction().resuming() && this.context.getTransaction().sizeLimitExceeded()) {
                            Collection<AugmentedEvent> augmentedEvents = this.context.getTransaction().clean();

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
            this.context.updatePostion();
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