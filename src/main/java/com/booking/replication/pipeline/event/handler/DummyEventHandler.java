package com.booking.replication.pipeline.event.handler;

import com.booking.replication.applier.ApplierException;
import com.booking.replication.binlog.event.RawBinlogEvent;
import com.booking.replication.pipeline.CurrentTransaction;
import com.booking.replication.schema.exception.TableMapException;

import java.io.IOException;

/**
 * Created by edmitriev on 7/12/17.
 */
public class DummyEventHandler implements RawBinlogEventHandler {

    @Override
    public void apply(RawBinlogEvent event, CurrentTransaction currentTransaction) throws TableMapException, ApplierException, IOException {
    }

    @Override
    public void handle(RawBinlogEvent event) throws TransactionException {
    }
}
