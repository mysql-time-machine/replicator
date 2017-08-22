package com.booking.replication.pipeline.event.handler;

import com.booking.replication.applier.ApplierException;
import com.booking.replication.pipeline.BinlogEventProducerException;
import com.booking.replication.pipeline.CurrentTransaction;
import com.booking.replication.schema.exception.TableMapException;
import com.google.code.or.binlog.BinlogEventV4;

import java.io.IOException;

/**
 * Created by edmitriev on 7/19/17.
 */
public interface BinlogEventV4Handler {
    void apply(BinlogEventV4 event, CurrentTransaction currentTransaction) throws ApplierException, EventHandlerApplyException, TableMapException, IOException;
    void handle(BinlogEventV4 event) throws TransactionException, BinlogEventProducerException, TransactionSizeLimitException;
}
