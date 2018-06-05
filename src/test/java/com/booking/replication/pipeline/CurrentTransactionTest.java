package com.booking.replication.pipeline;

import com.booking.replication.binlog.event.*;
import com.booking.replication.binlog.event.impl.BinlogEventQuery;
import com.booking.replication.binlog.event.impl.BinlogEventXid;
import com.booking.replication.binlog.event.impl.BinlogEventTableMap;
import com.google.code.or.binlog.impl.event.BinlogEventV4HeaderImpl;
import com.google.code.or.binlog.impl.event.QueryEvent;
import com.google.code.or.binlog.impl.event.TableMapEvent;
import com.google.code.or.binlog.impl.event.XidEvent;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Created by edmitriev on 8/1/17.
 */
public class CurrentTransactionTest {
    @Test
    public void getUuid() throws Exception {
        CurrentTransaction currentTransaction = new CurrentTransaction();
        assertNotNull(currentTransaction.getUuid());
    }

    @Test
    public void setXid() throws Exception {
        CurrentTransaction currentTransaction = new CurrentTransaction();
        assertEquals(0, currentTransaction.getXid());
        currentTransaction.setXid(Long.MAX_VALUE);
        assertEquals(Long.MAX_VALUE, currentTransaction.getXid());
    }


    // TODO: add same test for binlog connector
    @Test
    public void createCurrentTransactionWithBeginEvent() throws Exception {

        BinlogEventQuery queryEvent = new BinlogEventQuery(
                new QueryEvent(new BinlogEventV4HeaderImpl())
        );
        queryEvent.setSql("BEGIN");
        CurrentTransaction currentTransaction = new CurrentTransaction(queryEvent);
        assertTrue(currentTransaction.hasBeginEvent());
    }

    // TODO: add the same test for binlog connector
    @Test
    public void setFinishEvent() throws Exception {
        XidEvent xidEvent = new XidEvent(new BinlogEventV4HeaderImpl());
        BinlogEventXid rawBinlogEventXid = new BinlogEventXid(xidEvent);

        CurrentTransaction currentTransaction = new CurrentTransaction();
        currentTransaction.setFinishEvent(rawBinlogEventXid);

        assertTrue(currentTransaction.hasFinishEvent());
        assertNotNull(currentTransaction.getFinishEvent());
    }

    @Test
    public void setFinishEvent1() throws Exception {

        QueryEvent queryEvent = new QueryEvent(new BinlogEventV4HeaderImpl());
        BinlogEventQuery rawBinlogEventQuery = new BinlogEventQuery(queryEvent);

        rawBinlogEventQuery.setSql("COMMIT");

        CurrentTransaction currentTransaction = new CurrentTransaction();
        currentTransaction.setFinishEvent(rawBinlogEventQuery);

        assertTrue(currentTransaction.hasFinishEvent());
        assertNotNull(currentTransaction.getFinishEvent());
    }

    @Test
    public void addEvent() throws Exception {

        CurrentTransaction currentTransaction = new CurrentTransaction();
        assertEquals(0, currentTransaction.getEventsCounter());

        for (int i=0; i<5; i++) {
            QueryEvent queryEvent = new QueryEvent(new BinlogEventV4HeaderImpl());
            BinlogEventQuery rawBinlogEventQuery = new BinlogEventQuery(queryEvent);
            currentTransaction.addEvent(rawBinlogEventQuery);
        }
        assertEquals(5, currentTransaction.getEventsCounter());
    }

    @Test
    public void clearEvents() throws Exception {
        CurrentTransaction currentTransaction = new CurrentTransaction();
        assertEquals(0, currentTransaction.getEventsCounter());
        for (int i=0; i<5; i++) {
            QueryEvent queryEvent = new QueryEvent(new BinlogEventV4HeaderImpl());
            BinlogEventQuery rawBinlogEventQuery = new BinlogEventQuery(queryEvent);
            currentTransaction.addEvent(rawBinlogEventQuery);
        }
        assertEquals(5, currentTransaction.getEventsCounter());

        currentTransaction.clearEvents();
        assertEquals(0, currentTransaction.getEventsCounter());
    }

    // TODO: add the same test for binlog connector
    @Test
    public void doTimestampOverride() throws Exception {

        CurrentTransaction currentTransaction = new CurrentTransaction();

        for (int i = 0; i < 5; i++) {
            BinlogEventQuery queryEvent = new BinlogEventQuery(
                    new QueryEvent(new BinlogEventV4HeaderImpl())
            );
            currentTransaction.addEvent(queryEvent);
        }
        for (IBinlogEvent event: currentTransaction.getEvents()) {
            assertEquals(0, event.getTimestamp());
        }

        currentTransaction.setEventsTimestamp(Long.MAX_VALUE);

        for (IBinlogEvent event: currentTransaction.getEvents()) {
            assertEquals(Long.MAX_VALUE, event.getTimestamp());
        }

    }

    @Test
    public void setRewinded() throws Exception {
        CurrentTransaction currentTransaction = new CurrentTransaction();
        assertFalse(currentTransaction.isRewinded());
        currentTransaction.setRewinded(true);
        assertTrue(currentTransaction.isRewinded());
    }

    @Test
    public void hasMappingInTransaction() throws Exception {

        BinlogEventTableMap tableMapEvent = new BinlogEventTableMap(
                new TableMapEvent(new BinlogEventV4HeaderImpl())
        );

        tableMapEvent.setTableName("table");
        tableMapEvent.setDatabaseName("database");

        CurrentTransaction currentTransaction = new CurrentTransaction();

        assertFalse(currentTransaction.hasMappingInTransaction());

        currentTransaction.updateCache(tableMapEvent);

        assertTrue(currentTransaction.hasMappingInTransaction());
    }

}