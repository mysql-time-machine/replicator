package com.booking.replication.applier;

import com.booking.replication.augmenter.model.AugmentedEvent;
import com.booking.replication.augmenter.model.AugmentedEventData;
import com.booking.replication.augmenter.model.TableAugmentedEventData;
import com.booking.replication.augmenter.model.TransactionAugmentedEventData;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public interface Splitter extends Function<AugmentedEvent, List<AugmentedEvent>>, Closeable {
    enum Type {
        NONE {
            @Override
            public Splitter newInstance(Map<String, String> configuration) {
                return Collections::singletonList;
            }
        },
        TABLE_NAME {
            @Override
            public Splitter newInstance(Map<String, String> configuration) {
                return event -> {
                    if (TransactionAugmentedEventData.class.isInstance(event.getData())) {
                        TransactionAugmentedEventData transactionAugmentedEventData = TransactionAugmentedEventData.class.cast(event.getData());

                        Map<String, TransactionAugmentedEventData> augmentedEventDataMap = new HashMap<>();

                        for (AugmentedEvent augmentedEvent : transactionAugmentedEventData.getEventList()) {
                            if (TableAugmentedEventData.class.isInstance(augmentedEvent.getData())) {
                                TableAugmentedEventData tableAugmentedEventData = TableAugmentedEventData.class.cast(augmentedEvent.getData());

                                augmentedEventDataMap.computeIfAbsent(
                                        tableAugmentedEventData.getEventTable().toString(),
                                        key -> new TransactionAugmentedEventData(
                                                transactionAugmentedEventData.getIdentifier(),
                                                transactionAugmentedEventData.getXXID(),
                                                tableAugmentedEventData.getEventTable(),
                                                new ArrayList<>()
                                        )
                                ).getEventList().add(augmentedEvent);
                            }
                        }

                        List<AugmentedEvent> augmentedEventList = new ArrayList<>();

                        for (AugmentedEventData augmentedEventData : augmentedEventDataMap.values()) {
                            augmentedEventList.add(new AugmentedEvent(event.getHeader(), augmentedEventData));
                        }

                        return augmentedEventList;
                    } else {
                        return Collections.singletonList(event);
                    }
                };
            }
        };

        public abstract Splitter newInstance(Map<String, String> configuration);
    }

    interface Configuration {
        String TYPE = "splitter.type";
    }

    @Override
    default void close() throws IOException {
    }

    static Splitter build(Map<String, String> configuration) {
        return Splitter.Type.valueOf(
                configuration.getOrDefault(Configuration.TYPE, Type.NONE.name())
        ).newInstance(configuration);
    }
}
