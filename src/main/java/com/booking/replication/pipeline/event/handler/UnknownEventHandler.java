package com.booking.replication.pipeline.event.handler;

import com.booking.replication.applier.ApplierException;
import com.booking.replication.pipeline.CurrentTransaction;
import com.booking.replication.schema.exception.TableMapException;
import com.google.code.or.binlog.BinlogEventV4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created by edmitriev on 7/12/17.
 */
public class UnknownEventHandler implements BinlogEventV4Handler {
    private static final Logger LOGGER = LoggerFactory.getLogger(UnknownEventHandler.class);

    @Override
    public void apply(BinlogEventV4 event, CurrentTransaction currentTransaction) throws TableMapException, ApplierException, IOException {
        LOGGER.warn("Unexpected event type: " + event.getHeader().getEventType());
    }

    @Override
    public void handle(BinlogEventV4 event) throws TransactionException {
        LOGGER.warn("Unexpected event type: " + event.getHeader().getEventType());
    }
}
