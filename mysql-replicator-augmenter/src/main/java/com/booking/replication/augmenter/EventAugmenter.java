package com.booking.replication.augmenter;

import com.booking.replication.augmenter.active.schema.ActiveSchemaVersion;
import com.booking.replication.augmenter.exception.TableMapException;
import com.booking.replication.model.Event;
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


    public Event mapDataEventToSchema(Event abstractRowEvent, TransactionEventData currentTransaction) throws Exception {
        Event au = null;

        switch (abstractRowEvent.getHeader().getEventType()) {
            case UPDATE_ROWS:
                break;

            case WRITE_ROWS:
                break;

            case DELETE_ROWS:
                break;

            default:
                throw new TableMapException("RBR event type expected! Received type: " +
                        abstractRowEvent.getHeader().getEventType().toString(), abstractRowEvent
                );
        }

        if (au == null) {
            throw  new TableMapException("Augmented event ended up as null - something went wrong!", abstractRowEvent);
        }

        return au;
    }

    @Override
    public Event apply(Event event) {
        return event;
//        Event augmentedEvent = null;
//        try {
//            augmentedEvent = mapDataEventToSchema(event, null);
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//        return augmentedEvent;
    }
}
