package com.booking.replication.pipeline.event.handler;

import com.booking.replication.applier.ApplierException;
import com.booking.replication.applier.kafka.KafkaMessageBufferException;
import com.booking.replication.binlog.event.IBinlogEvent;
import com.booking.replication.exceptions.RowListMessageSerializationException;
import com.booking.replication.pipeline.BinlogEventProducerException;
import com.booking.replication.pipeline.CurrentTransaction;
import com.booking.replication.schema.exception.TableMapException;

import java.io.IOException;

/**
 * Created by edmitriev on 7/19/17.
 */
public interface RawBinlogEventHandler {
    void apply(IBinlogEvent event, CurrentTransaction currentTransaction)
            throws ApplierException, EventHandlerApplyException, TableMapException, IOException, RowListMessageSerializationException, KafkaMessageBufferException;
    void handle(IBinlogEvent event)
            throws TransactionException, BinlogEventProducerException, TransactionSizeLimitException;
}
