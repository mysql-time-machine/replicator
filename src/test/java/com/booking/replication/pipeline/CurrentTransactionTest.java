package com.booking.replication.pipeline;

import com.google.code.or.binlog.BinlogEventV4;
import com.google.code.or.binlog.impl.event.BinlogEventV4HeaderImpl;
import com.google.code.or.binlog.impl.event.QueryEvent;
import com.google.code.or.binlog.impl.event.TableMapEvent;
import com.google.code.or.binlog.impl.event.XidEvent;
import com.google.code.or.common.glossary.column.StringColumn;
import org.junit.Test;

import java.lang.reflect.Constructor;

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

    @Test
    public void createCurrentTransactionWithBeginEvent() throws Exception {
        QueryEvent queryEvent = new QueryEvent(new BinlogEventV4HeaderImpl());
        Constructor<StringColumn> stringColumnReflection = StringColumn.class.getDeclaredConstructor(byte[].class);
        stringColumnReflection.setAccessible(true);
        StringColumn stringColumn = stringColumnReflection.newInstance((Object) "BEGIN".getBytes());
        queryEvent.setSql(stringColumn);

        CurrentTransaction currentTransaction = new CurrentTransaction(queryEvent);
        assertTrue(currentTransaction.hasBeginEvent());
    }

    @Test
    public void setFinishEvent() throws Exception {
        XidEvent xidEvent = new XidEvent(new BinlogEventV4HeaderImpl());
        CurrentTransaction currentTransaction = new CurrentTransaction();
        currentTransaction.setFinishEvent(xidEvent);
    }

    @Test
    public void setFinishEvent1() throws Exception {
        QueryEvent queryEvent = new QueryEvent(new BinlogEventV4HeaderImpl());
        Constructor<StringColumn> stringColumnReflection = StringColumn.class.getDeclaredConstructor(byte[].class);
        stringColumnReflection.setAccessible(true);
        StringColumn stringColumn = stringColumnReflection.newInstance((Object) "COMMIT".getBytes());
        queryEvent.setSql(stringColumn);

        CurrentTransaction currentTransaction = new CurrentTransaction();
        currentTransaction.setFinishEvent(queryEvent);
        assertTrue(currentTransaction.hasFinishEvent());
        assertNotNull(currentTransaction.getFinishEvent());
    }

    @Test
    public void addEvent() throws Exception {
        CurrentTransaction currentTransaction = new CurrentTransaction();
        assertEquals(0, currentTransaction.getEventsCounter());
        for (int i=0; i<5; i++) {
            currentTransaction.addEvent(new QueryEvent());
        }
        assertEquals(5, currentTransaction.getEventsCounter());
    }

    @Test
    public void clearEvents() throws Exception {
        CurrentTransaction currentTransaction = new CurrentTransaction();
        assertEquals(0, currentTransaction.getEventsCounter());
        for (int i=0; i<5; i++) {
            currentTransaction.addEvent(new QueryEvent());
        }
        assertEquals(5, currentTransaction.getEventsCounter());
        currentTransaction.clearEvents();
        assertEquals(0, currentTransaction.getEventsCounter());
    }

    @Test
    public void doTimestampOverride() throws Exception {
        CurrentTransaction currentTransaction = new CurrentTransaction();
        for (int i = 0; i < 5; i++) {
            QueryEvent queryEvent = new QueryEvent(new BinlogEventV4HeaderImpl());
            currentTransaction.addEvent(queryEvent);
        }
        for (BinlogEventV4 event: currentTransaction.getEvents()) {
            assertEquals(0, event.getHeader().getTimestamp());
        }
        currentTransaction.setEventsTimestamp(Long.MAX_VALUE);
        for (BinlogEventV4 event: currentTransaction.getEvents()) {
            assertEquals(Long.MAX_VALUE, event.getHeader().getTimestamp());
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
        TableMapEvent tableMapEvent = new TableMapEvent(new BinlogEventV4HeaderImpl());
        Constructor<StringColumn> stringColumnReflection = StringColumn.class.getDeclaredConstructor(byte[].class);
        stringColumnReflection.setAccessible(true);
        StringColumn stringColumn = stringColumnReflection.newInstance((Object) "table".getBytes());
        tableMapEvent.setTableName(stringColumn);
        tableMapEvent.setDatabaseName(stringColumn);

        CurrentTransaction currentTransaction = new CurrentTransaction();
        assertFalse(currentTransaction.hasMappingInTransaction());
        currentTransaction.updateCache(tableMapEvent);
        assertTrue(currentTransaction.hasMappingInTransaction());
    }

}