package com.booking.replication.augmenter;

import com.booking.replication.augmenter.active.schema.MySQLActiveSchemaVersion;
import com.booking.replication.augmenter.exception.TableMapException;
import com.booking.replication.augmenter.model.AugmentedEvent;
import com.booking.replication.augmenter.model.AugmentedEventData;
import com.booking.replication.augmenter.model.AugmentedEventHeader;
import com.booking.replication.augmenter.model.AugmentedEventImplementation;
import com.booking.replication.augmenter.transaction.CurrentTransaction;
import com.booking.replication.supplier.model.RawEvent;
import com.booking.replication.supplier.model.handler.JSONInvocationHandler;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.URISyntaxException;
import java.sql.SQLException;

import static com.booking.replication.supplier.model.RawEventType.DELETE_ROWS;
import static com.booking.replication.supplier.model.RawEventType.UPDATE_ROWS;
import static com.booking.replication.supplier.model.RawEventType.WRITE_ROWS;


public class EventAugmenter implements Augmenter {

    public final static String UUID_FIELD_NAME = "_replicator_uuid";
    public final static String XID_FIELD_NAME  = "_replicator_xid";

    private MySQLActiveSchemaVersion mySQLActiveSchemaVersion;
    private final boolean            applyUuid;
    private final boolean            applyXid;

    private static final Logger LOGGER = LogManager.getLogger(EventAugmenter.class);

    public EventAugmenter(
            MySQLActiveSchemaVersion asv,
            boolean                  applyUuid,
            boolean                  applyXid
    ) throws SQLException, URISyntaxException {
            mySQLActiveSchemaVersion = asv;
            this.applyUuid           = applyUuid;
            this.applyXid            = applyXid;
    }

    @Override
    public AugmentedEvent apply(RawEvent rawEvent) {

        EventAugmenter.LOGGER.info("transforming event");

        AugmentedEvent augmentedEvent = null;
        try {
            augmentedEvent = mapDataEventToSchema(rawEvent, null);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return augmentedEvent;
    }

    public AugmentedEvent mapDataEventToSchema(RawEvent rawEvent, CurrentTransaction currentTransaction
    ) throws Exception {

        AugmentedEvent au = null;

        switch (rawEvent.getHeader().getRawEventType()) {

            // TODO: IMPLEMENT
            case UPDATE_ROWS:
                break;

            case WRITE_ROWS:
                break;

            case DELETE_ROWS:
                break;

            default:
                au = augmentUnsupportedEvent(rawEvent);
        }

        if (au == null) {
            throw new TableMapException(
                    "Augmented event ended up as null - something went wrong!",
                    rawEvent
            );
        }

        return au;
    }

    private AugmentedEvent augmentUnsupportedEvent(RawEvent rawEvent) throws
            IOException,
            InvocationTargetException,
            NoSuchMethodException,
            InstantiationException,
            IllegalAccessException
    {
        // make augHeader
        AugmentedEventHeader dummyHeader = AugmentedEventHeader.getProxy(
                new JSONInvocationHandler(
                        String.format(
                                "{\"timestamp\": %s, \"eventType\": %s, \"tableName\": \"UNKNOWN\"}",
                                System.currentTimeMillis(),
                                rawEvent.getHeader().getRawEventType().toString()
                        ).getBytes())
        );

        // make augData
        AugmentedEventData dummyData = null;

        AugmentedEvent au = new AugmentedEventImplementation(dummyHeader, dummyData);

        return au;
    }
}
