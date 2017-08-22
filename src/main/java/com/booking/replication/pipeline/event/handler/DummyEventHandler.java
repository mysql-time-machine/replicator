package com.booking.replication.pipeline.event.handler;

import com.booking.replication.applier.ApplierException;
import com.booking.replication.pipeline.CurrentTransaction;
import com.booking.replication.schema.exception.TableMapException;
import com.google.code.or.binlog.BinlogEventV4;

import java.io.IOException;

/**
 * Created by edmitriev on 7/12/17.
 */
public class DummyEventHandler implements BinlogEventV4Handler {

    @Override
    public void apply(BinlogEventV4 event, CurrentTransaction currentTransaction) throws TableMapException, ApplierException, IOException {
    }

    @Override
    public void handle(BinlogEventV4 event) throws TransactionException {
    }
}
