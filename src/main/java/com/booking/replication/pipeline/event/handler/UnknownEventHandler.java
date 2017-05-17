package com.booking.replication.pipeline.event.handler;

import com.booking.replication.applier.ApplierException;
import com.booking.replication.binlog.event.RawBinlogEvent;
import com.booking.replication.pipeline.CurrentTransaction;
import com.booking.replication.schema.exception.TableMapException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created by edmitriev on 7/12/17.
 */
public class UnknownEventHandler implements RawBinlogEventHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(UnknownEventHandler.class);

    @Override
    public void apply(RawBinlogEvent event, CurrentTransaction currentTransaction)
            throws TableMapException, ApplierException, IOException {
        LOGGER.warn("Unexpected event type: " + event.getEventType());
    }

    @Override
    public void handle(RawBinlogEvent event) throws TransactionException {
        LOGGER.warn("Unexpected event type: " + event.getEventType());
    }
}
