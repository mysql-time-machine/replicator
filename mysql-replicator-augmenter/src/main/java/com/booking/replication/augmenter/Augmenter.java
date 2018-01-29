package com.booking.replication.augmenter;

import com.booking.replication.augmenter.active.schema.ActiveSchemaVersion;
import com.booking.replication.mysql.binlog.model.EventData;
import com.booking.replication.mysql.binlog.model.augmented.AugmentedEventData;
import com.booking.replication.mysql.binlog.model.transaction.TransactionEventData;

import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.Map;
import com.booking.replication.mysql.binlog.model.Event;
import java.util.function.Function;

public interface Augmenter extends Function<Event, Event> {
    interface Configuration {
        String ACTIVE_SCHEMA = "augmenter.active.schema";
        String APPLY_UUID = "augmenter.apply.uuid";
        String APPLY_XID = "augmenter.apply.xid";
    }

    @SuppressWarnings("unchecked")
    static Augmenter build(Map<String, String> configuration) throws IOException, SQLException, URISyntaxException {
        return new EventAugmenter(new ActiveSchemaVersion(configuration), Boolean.parseBoolean(configuration.get(Configuration.APPLY_UUID)), Boolean.parseBoolean(configuration.get(Configuration.APPLY_XID)));
    }
}