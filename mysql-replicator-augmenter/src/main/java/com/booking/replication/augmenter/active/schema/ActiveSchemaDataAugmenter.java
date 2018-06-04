package com.booking.replication.augmenter.active.schema;

import com.booking.replication.augmenter.model.AugmentedEventData;
import com.booking.replication.augmenter.model.TransactionAugmentedEventData;
import com.booking.replication.supplier.model.RawEvent;

import java.util.Map;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;

public class ActiveSchemaDataAugmenter  implements Function<RawEvent, AugmentedEventData> {
    public interface Configuration extends ActiveSchemaAugmenter.Configuration {
        String APPLY_UUID = "augmenter.apply.uuid";
        String APPLY_XID = "augmenter.apply.xid";
        String UUID_FIELD_NAME = "_replicator_uuid";
        String XID_FIELD_NAME = "_replicator_xid";
    }

    private static final Logger LOG = Logger.getLogger(ActiveSchemaDataAugmenter.class.getName());

    private final ActiveSchemaContext context;

    public ActiveSchemaDataAugmenter(ActiveSchemaContext context) {
        this.context = context;
    }

    @Override
    public AugmentedEventData apply(RawEvent rawEvent) {
        if (this.context.getTransaction().started()) {
            if (!this.context.getTransaction().add(this.getAugmentedEventData(rawEvent))) {
                ActiveSchemaDataAugmenter.LOG.log(Level.WARNING, "cannot add to transaction");
            }

            return null;
        } else if (this.context.getTransaction().committed()) {
            return this.context.getTransaction().getData();
        } else {
            return null;
        }
    }

    private AugmentedEventData getAugmentedEventData(RawEvent rawEvent) {
        return null;
    }
}
