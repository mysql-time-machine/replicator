package com.booking.replication.augmenter;

import com.booking.replication.augmenter.filters.TableNameMergePatternFilter;
import com.booking.replication.augmenter.model.event.AugmentedEvent;
import java.util.Collection;
import java.util.Map;
import java.util.function.Function;

public interface AugmenterFilter extends Function<Collection<AugmentedEvent>,Collection<AugmentedEvent>> {

    enum Type {

        IDENTITY {
            @Override
            protected AugmenterFilter newInstance(Map<String, Object> configuration) {
                return (augmentedEvents) -> augmentedEvents;
            }
        },

        TABLE_MERGE_PATTERN {
            @Override
            protected AugmenterFilter newInstance(Map<String, Object> configuration) {
                return new TableNameMergePatternFilter(configuration);
            }
        };

        protected abstract AugmenterFilter newInstance(Map<String, Object> configuration);
    }

    interface Configuration {
            String FILTER_TYPE = "augmenter.filter.type";
            String FILTER_CONFIGURATION = "augmenter.filter.pattern";
    }

    @SuppressWarnings("unchecked")
    static AugmenterFilter build(Map<String, Object> configuration) {
        return Type.valueOf(
                configuration.getOrDefault(
                        Configuration.FILTER_TYPE,
                        Type.IDENTITY.name()
                ).toString()
        ).newInstance(configuration);
    }
}
