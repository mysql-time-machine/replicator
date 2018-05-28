package com.booking.replication.augmenter.transaction;


import com.booking.replication.augmenter.pipeline.BinlogPositionComparationException;
import com.booking.replication.augmenter.pipeline.BinlogPositionInfo;
import com.booking.replication.augmenter.pipeline.EventPosition;

import com.booking.replication.augmenter.sql.QueryEventType;
import com.booking.replication.augmenter.pipeline.TransactionException;

import com.booking.replication.augmenter.pipeline.TableMapException;

import com.booking.replication.augmenter.sql.QueryInspector;

import com.booking.replication.supplier.model.*;
import com.google.code.or.binlog.BinlogEventV4;
import com.google.code.or.binlog.impl.event.BinlogEventV4HeaderImpl;
import com.google.code.or.binlog.impl.event.QueryEvent;
import com.google.code.or.binlog.impl.event.TableMapEvent;
import com.google.code.or.binlog.impl.event.XidEvent;
import com.google.common.base.Joiner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Created by bosko on 11/10/15.
 */
public class CurrentTransaction {

    private static final Logger LOGGER = LoggerFactory.getLogger(CurrentTransaction.class);

    public static final long FAKEXID = 0;

    private long                         xid;
    private final UUID                   uuid            = UUID.randomUUID();
    private final Map<Long,String>       tableID2Name    = new HashMap<>();
    private final Map<Long, String>      tableID2DBName  = new HashMap<>();
    private       RawEvent               beginEvent      = null;
    private       RawEvent               finishEvent     = null;
    private       boolean                isRewinded      = false;

    private RawEvent                     firstMapEventInTransaction = null;

    private Queue<RawEvent> events = new LinkedList<>();

    private final Map<String, RawEvent> currentTransactionTableMapEvents = new HashMap<>();

    public CurrentTransaction() {
    }

    public CurrentTransaction(RawEvent event) {
        if (!QueryEventType.BEGIN.equals(QueryInspector.getQueryEventType((QueryEventData)event.getData()))) {
            throw new RuntimeException("Can't set beginEvent for transaction to a wrong event type: " + event);
        }
        beginEvent = event;
    }

    @Override
    public String toString() {
        String beginEventBinlogFilename = beginEvent == null ? null : EventPosition.getEventBinlogFileName(beginEvent);
        Long beginEventBinlogPosition = beginEvent == null ? null : EventPosition.getEventBinlogPosition(beginEvent);
        return "uuid: " + uuid +
                ", xid: " + xid +
                ", tableID2Name: " + tableID2Name +
                ", tableID2DBName: " + tableID2DBName +
                ", beginBinlogFileName: " + beginEventBinlogFilename +
                ", beginBinlogPosition: " + beginEventBinlogPosition +
                ", events size: " + events.size() +
                ", beginEvent: " + beginEvent +
                ", finishEvent: " + finishEvent +
                ", events:\n    - " + Joiner.on("\n    - ").join(events);
    }

    public UUID getUuid() {
        return uuid;
    }

    public long getXid() {
        return xid;
    }

    void setXid(long xid) {
        this.xid = xid;
    }

    QueryEventData getBeginEventData() {
        return (QueryEventData) beginEvent.getData();
    }

    void setFinishEvent (RawEvent finishEvent)  throws TransactionException {
        if (finishEvent.getHeader().getRawEventType().equals(RawEventType.XID)) {
            setFinishEventXid(finishEvent);
        }
        if (finishEvent.getHeader().getRawEventType().equals(RawEventType.QUERY)) {
            setFinishEventQuery(finishEvent);
        }
    }

    void setFinishEventXid(RawEvent finishEvent) throws TransactionException {
        // xa-capable engines block (InnoDB)
        if (this.finishEvent == null) {
            setXid(((XIDEventData)finishEvent.getData()).getXID());
            this.finishEvent = finishEvent;
            return;
        }
        if (isRewinded) {
            if (
                ((XIDEventData) this.finishEvent.getData()).getXID()
                 !=
                ((XIDEventData) finishEvent.getData()).getXID()
            ) {
                throw new TransactionException(
                        "XidEvents must match if in rewinded transaction. Old: " +
                                this.finishEvent +
                                ", new: " +
                                finishEvent
                );
            }
        } else {
            throw new TransactionException("Trying to set a finish event twice. Old: " + this.finishEvent + ", new: " + finishEvent);
        }
    }

    void setFinishEventQuery(RawEvent finishEvent) throws TransactionException {
        // MyIsam block
        if (!QueryEventType.COMMIT.equals(QueryInspector.getQueryEventType((QueryEventData) finishEvent.getData()))) {
            throw new TransactionException("Can't set finishEvent for transaction to a wrong event type: " + finishEvent);
        }
        if (this.finishEvent == null) {
            setXid(FAKEXID);
            this.finishEvent = finishEvent;
            return;
        }
        if (isRewinded) {
            BinlogPositionInfo oldFinishEventPosition = new BinlogPositionInfo(
                    this.finishEvent.getHeader().getBinlogFileName(),
                    this.finishEvent.getHeader().getBinlogPosition()
            );
            BinlogPositionInfo newFinishEventPosition = new BinlogPositionInfo(
                    finishEvent.getHeader().getBinlogFileName(),
                    finishEvent.getHeader().getBinlogPosition()
            );
            try {
                if (BinlogPositionInfo.compare(oldFinishEventPosition, newFinishEventPosition) != 0) {
                    throw new TransactionException("XidEvents must match if in rewinded transaction. Old: " + this.finishEvent + ", new: " + finishEvent);
                }
            } catch (BinlogPositionComparationException e) {
                throw new TransactionException("Failed to compare old and new commit events. Old: " + this.finishEvent + ", new: " + finishEvent, e);
            }
        } else {
            throw new TransactionException("Trying to set a finish event twice. Old: " + this.finishEvent + ", new: " + finishEvent);
        }
    }

    RawEvent getFinishEvent() {
        return finishEvent;
    }

    boolean hasBeginEvent() {
        return (
                (beginEvent != null)
                &&
                ((QueryEventData)beginEvent.getData()).getSQL().equals("BEGIN")
        );
    }

    boolean hasFinishEvent() {
        return (finishEvent != null);
    }

    void addEvent(RawEvent event) {
        events.add(event);
    }

    boolean hasEvents() {
        return (events.peek() != null);
    }

    public Queue<RawEvent> getEvents() {
        return events;
    }

    long getEventsCounter() {
        return events.size();
    }

    void clearEvents() {
        events = new LinkedList<>();
    }

    void setBeginEventTimestamp(long timestamp) throws TransactionException {
        beginEvent.setTimestamp(timestamp);
    }

    void setBeginEventTimestampToFinishEvent() throws TransactionException {
        if (!hasFinishEvent()) {
            throw new TransactionException("Can't set timestamp to timestamp of finish event while no finishEvent exists");
        }
        setBeginEventTimestamp(finishEvent.getTimestamp());
    }

    void setEventsTimestamp(long timestamp) throws TransactionException {
        for (RawEvent event : events) {
            event.overrideTimestamp(timestamp);
        }
    }

    void setEventsTimestampToFinishEvent() throws TransactionException {
        if (!hasFinishEvent()) {
            throw new TransactionException("Can't set timestamp to timestamp of finish event while no finishEvent exists");
        }
        setEventsTimestamp(finishEvent.getTimestamp());
    }

    /**
     * Update table map cache.
     * @param event event
     */
    public void updateCache(RawEvent event) {
        LOGGER.debug("Updating cache. firstMapEventInTransaction: "
                + firstMapEventInTransaction + ", event: " + event);
        if (firstMapEventInTransaction == null) {
            firstMapEventInTransaction = event;
        }

        String tableName = ((TableMapEventData)event.getData()).getTable();

        tableID2Name.put(
                ((TableMapEventData)event.getData()).getTableId(),
                tableName
        );

        tableID2DBName.put(
                ((TableMapEventData)event.getData()).getTableId(),
                ((TableMapEventData)event.getData()).getDatabase()
        );

        currentTransactionTableMapEvents.put(tableName, event);
    }

    /**
     * Map table id to table name.
     *
     * @param tableID table id
     * @return table name
     * @throws TableMapException Table ID not present in CurrentTransaction
     */
    public String getTableNameFromID(Long tableID) throws TableMapException {
        if (! tableID2DBName.containsKey(tableID)) {
            LOGGER.error(String.format(
                    "Table ID not known. Known tables and ids are: %s",
                    Joiner.on(" ").join(tableID2DBName.keySet(), " ")));
            throw new TableMapException("Table ID not present in CurrentTransaction!");
        }

        return tableID2Name.get(tableID);
    }

    /**
     * Map table id to schema name.
     *
     * @param tableID table id
     *
     * @return schema name
     * @throws TableMapException Table ID not present in CurrentTransaction
     */
    public String getDBNameFromTableID(Long tableID) throws TableMapException {
        String dbName = tableID2DBName.get(tableID);

        if (dbName == null) {
            throw new TableMapException("Table ID not present in CurrentTransaction!");
        } else {
            return dbName;
        }
    }

    public boolean isRewinded() {
        return isRewinded;
    }

    void setRewinded(boolean rewinded) {
        isRewinded = rewinded;
    }

    public RawEvent getTableMapEvent(String tableName) {
        return currentTransactionTableMapEvents.get(tableName);
    }

    public RawEvent getFirstMapEventInTransaction() {
        return firstMapEventInTransaction;
    }

    boolean hasMappingInTransaction() {
        return (firstMapEventInTransaction != null)
                && (((TableMapEventData)firstMapEventInTransaction.getData()).getDatabase() != null)
                && (((TableMapEventData)firstMapEventInTransaction.getData()).getTable() != null);
    }

    Map<String, RawEvent> getCurrentTransactionTableMapEvents() {
        return currentTransactionTableMapEvents;
    }

}