package com.booking.replication.augmenter;

import com.booking.replication.augmenter.active.schema.ActiveSchemaVersion;
import com.booking.replication.model.Event;
import com.booking.replication.model.EventData;
import com.booking.replication.model.augmented.AugmentedEventData;
import com.booking.replication.model.transaction.TransactionEventData;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.URISyntaxException;
import java.sql.SQLException;


public class EventAugmenter implements Augmenter {
    public final static String UUID_FIELD_NAME = "_replicator_uuid";
    public final static String XID_FIELD_NAME = "_replicator_xid";

    private ActiveSchemaVersion activeSchemaVersion;
    private final boolean applyUuid;
    private final boolean applyXid;

    private static final Logger LOGGER = LogManager.getLogger(EventAugmenter.class);

    public EventAugmenter(ActiveSchemaVersion asv, boolean applyUuid, boolean applyXid) throws SQLException, URISyntaxException {
        activeSchemaVersion = asv;
        this.applyUuid = applyUuid;
        this.applyXid = applyXid;
    }


    public AugmentedEventData mapDataEventToSchema(EventData abstractRowEvent, TransactionEventData currentTransaction) throws Exception {
        return null;
    }

    @Override
    public Event apply(Event event) {
        return null;
    }
}
