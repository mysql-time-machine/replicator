package com.booking.replication.augmenter;

import com.booking.replication.augmenter.active.schema.ActiveSchemaVersion;
import com.booking.replication.augmenter.model.AugmentedEvent;
import com.booking.replication.supplier.model.RawEvent;

import java.util.Map;
import java.util.function.Function;

public interface Augmenter extends Function<RawEvent, AugmentedEvent> {

    enum Type {

        NONE {
            @Override
            public Augmenter newInstance(Map<String, String> configuration)
            {
                return event -> null;
            }
        },

        PSEUDO_GTID {
            @Override
            public Augmenter newInstance(Map<String, String> configuration) {
                return new PseudoGTIDAugmenter(configuration);
            }
        }, // TODO: add augmenter chaining: augmenter = Augmenter.with(Pseudo).then(Other)

        EVENT {

            @Override
            public Augmenter newInstance(Map<String, String> configuration) {
                try {
                    return new EventAugmenter(
                            new ActiveSchemaVersion(configuration),
                            Boolean.parseBoolean(configuration.get(Configuration.APPLY_UUID)),
                            Boolean.parseBoolean(configuration.get(Configuration.APPLY_XID))
                    );
                } catch (Exception exception) {
                    throw new RuntimeException(exception);
                }
            }
        };

        public abstract Augmenter newInstance(Map<String, String> configuration);

    }

    interface Configuration {
        String TYPE                = "augmenter.type";
        String PSEUDO_GTID_PATTERN = "augmenter.pseudogtid.pattern";
        String ACTIVE_SCHEMA       = "augmenter.active.schema";
        String APPLY_UUID          = "augmenter.apply.uuid";
        String APPLY_XID           = "augmenter.apply.xid";
    }

    static Augmenter build(Map<String, String> configuration) {
        return Augmenter.Type.valueOf(
                configuration.getOrDefault(
                        Configuration.TYPE,
                        Type.NONE.name()
                )
        ).newInstance(configuration);
    }
}