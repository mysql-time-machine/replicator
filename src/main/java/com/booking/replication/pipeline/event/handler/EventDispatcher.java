package com.booking.replication.pipeline.event.handler;


import com.booking.replication.binlog.event.BinlogEventType;
import com.booking.replication.binlog.event.IBinlogEvent;
import com.booking.replication.pipeline.BinlogEventProducerException;
import com.booking.replication.pipeline.CurrentTransaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by edmitriev on 7/13/17.
 */
public class EventDispatcher implements RawBinlogEventHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(EventDispatcher.class);

    private final Map<BinlogEventType, RawBinlogEventHandler> handlers = new HashMap<>();

    private final UnknownEventHandler unknownEventHandler = new UnknownEventHandler();


    public void registerHandler(BinlogEventType type, RawBinlogEventHandler handler) {
        handlers.put(type, handler);
    }

    public void registerHandler(List<BinlogEventType> types, RawBinlogEventHandler handler) {
        for (BinlogEventType type : types) {
            handlers.put(type, handler);
        }
    }

    private RawBinlogEventHandler getHandler(BinlogEventType type) {
        return handlers.getOrDefault(type, unknownEventHandler);
    }

    @Override
    public void apply(IBinlogEvent event, CurrentTransaction currentTransaction)
            throws EventHandlerApplyException {
        try {
            RawBinlogEventHandler eventHandler = getHandler(event.getEventType());

            LOGGER.debug("Applying event: " + event + ", handler: " + eventHandler);
            eventHandler.apply(event, currentTransaction);

        } catch (Exception e) {
            throw new EventHandlerApplyException("Failed to apply event:", e);
        }
    }

    @Override
    public void handle(IBinlogEvent event) throws TransactionException, TransactionSizeLimitException {
        LOGGER.debug("Handling event: " + event);

        try {
            LOGGER.debug("trying to acquire handler for { event type => " + event.getEventType() + ", class type => " + event.getClass().toString());
            RawBinlogEventHandler eventHandler = getHandler(event.getEventType());
            eventHandler.handle(event);

        } catch (TransactionSizeLimitException e) {
            throw e;
        } catch (BinlogEventProducerException e) {
            throw new TransactionException("Failed to handle event: ", e);
        }
    }
}
