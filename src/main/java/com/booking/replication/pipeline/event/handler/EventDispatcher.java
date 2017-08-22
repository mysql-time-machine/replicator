package com.booking.replication.pipeline.event.handler;


import com.booking.replication.pipeline.BinlogEventProducerException;
import com.booking.replication.pipeline.CurrentTransaction;
import com.google.code.or.binlog.BinlogEventV4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by edmitriev on 7/13/17.
 */
public class EventDispatcher implements BinlogEventV4Handler {
    private static final Logger LOGGER = LoggerFactory.getLogger(EventDispatcher.class);

    private final Map<Integer, BinlogEventV4Handler> handlers = new HashMap<>();
    private final UnknownEventHandler unknownEventHandler = new UnknownEventHandler();


    public void registerHandler(Integer type, BinlogEventV4Handler handler) {
        handlers.put(type, handler);
    }

    public void registerHandler(List<Integer> types, BinlogEventV4Handler handler) {
        for (Integer type : types) {
            handlers.put(type, handler);
        }
    }

    private BinlogEventV4Handler getHandler(Integer type) {
        return handlers.getOrDefault(type, unknownEventHandler);
    }

    @Override
    public void apply(BinlogEventV4 event, CurrentTransaction currentTransaction) throws EventHandlerApplyException {
        try {
            BinlogEventV4Handler eventHandler = getHandler(event.getHeader().getEventType());
            LOGGER.debug("Applying event: " + event + ", handler: " + eventHandler);
            eventHandler.apply(event, currentTransaction);
        } catch (Exception e) {
            throw new EventHandlerApplyException("Failed to apply event: ", e);
        }
    }

    @Override
    public void handle(BinlogEventV4 event) throws TransactionException, TransactionSizeLimitException {
        LOGGER.debug("Handling event: " + event);
        try {
            BinlogEventV4Handler eventHandler = getHandler(event.getHeader().getEventType());
            eventHandler.handle(event);
        } catch (TransactionSizeLimitException e) {
            throw e;
        } catch (BinlogEventProducerException e) {
            throw new TransactionException("Failed to handle event: ", e);
        }
    }
}
