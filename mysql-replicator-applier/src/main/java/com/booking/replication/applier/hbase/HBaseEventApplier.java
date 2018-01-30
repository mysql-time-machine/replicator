package com.booking.replication.applier.hbase;

import com.booking.replication.applier.EventApplier;
import com.booking.replication.mysql.binlog.model.*;
import com.booking.replication.mysql.binlog.model.augmented.AugmentedEventData;
import com.booking.replication.mysql.binlog.model.transaction.TransactionEventData;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

@SuppressWarnings("unused")
public class HBaseEventApplier implements EventApplier {
    interface Configuration {
        String SCHEMA = "hbase.schema";
        String ZOOKEEPER_QUORUM = "hbase.zookeeper.quorum";
    }

    private static final Logger log = Logger.getLogger(HBaseEventApplier.class.getName());

    private final Connection connection;

    public HBaseEventApplier(Map<String, String> configuration) {
        try {
            org.apache.hadoop.conf.Configuration hbaseConfiguration = HBaseConfiguration.create();

            hbaseConfiguration.set(Configuration.ZOOKEEPER_QUORUM, configuration.get(Configuration.ZOOKEEPER_QUORUM));

            this.connection = ConnectionFactory.createConnection(hbaseConfiguration);
        } catch (IOException exception) {
            throw new UncheckedIOException(exception);
        }
    }

    @Override
    public void accept(Event event) {
        try {
            switch (event.getHeader().getEventType()) {
                case TRANSACTION:
                    this.handleTransactionEvent(event.getHeader(), event.getData());
                    break;
                case AUGMENTED_INSERT:
                    this.handleAugmentedEvent(event.getHeader(), event.getData());
                    break;
                default:
                    this.handleUnknownEvent(event.getHeader(), event.getData());
                    break;
            }
        } catch (IOException exception) {
            throw new UncheckedIOException(exception);
        }
    }

    private Table createTable() {
        return null;
    }

    private void handleTransactionEvent(EventHeader header, TransactionEventData data) {
        for (Event event : data.getEvents()) {
            this.accept(event);
        }
    }

    private void handleAugmentedEvent(EventHeader header, AugmentedEventData data) throws IOException {
    }

    private void handleUnknownEvent(EventHeader header, EventData data) {
        HBaseEventApplier.log.log(Level.FINE, "Unknown event type {}", header.getEventType().name());
    }

    @Override
    public void close() throws IOException {
        if (this.connection != null) {
            this.connection.close();
        }
    }
}
