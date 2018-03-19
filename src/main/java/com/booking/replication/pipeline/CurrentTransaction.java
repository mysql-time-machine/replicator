package com.booking.replication.pipeline;

import com.booking.replication.binlog.EventPosition;
import com.booking.replication.binlog.event.QueryEventType;
import com.booking.replication.pipeline.event.handler.TransactionException;
import com.booking.replication.schema.exception.TableMapException;
import com.booking.replication.sql.QueryInspector;
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
 * Class that represents the current transaction
 *
 * There is an assumption that all tables in the transaction
 * are from the same database. Cross database transactions
 * are not supported.
 */
public class CurrentTransaction {

    private static final Logger LOGGER = LoggerFactory.getLogger(CurrentTransaction.class);

    public static final long FAKEXID = 0;

    private final UUID uuid = UUID.randomUUID();
    private long xid;
    private final Map<Long,String> tableID2Name = new HashMap<>();
    private final Map<Long, String> tableID2DBName = new HashMap<>();
    private QueryEvent beginEvent = null;
    private BinlogEventV4 finishEvent = null;
    private boolean isRewinded = false;

    private TableMapEvent firstMapEventInTransaction = null;
    private Queue<BinlogEventV4> events = new LinkedList<>();

    private final Map<String, TableMapEvent> currentTransactionTableMapEvents = new HashMap<>();

    public CurrentTransaction() {
    }

    public CurrentTransaction(QueryEvent event) {
        if (!QueryEventType.BEGIN.equals(QueryInspector.getQueryEventType(event))) {
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

    QueryEvent getBeginEvent() {
        return beginEvent;
    }


    void setFinishEvent(XidEvent finishEvent) throws TransactionException {
        // xa-capable engines block (InnoDB)
        if (this.finishEvent == null) {
            setXid(finishEvent.getXid());
            this.finishEvent = finishEvent;
            return;
        }
        if (isRewinded) {
            if (((XidEvent) this.finishEvent).getXid() != finishEvent.getXid()) {
                throw new TransactionException("XidEvents must match if in rewinded transaction. Old: " + this.finishEvent + ", new: " + finishEvent);
            }
        } else {
            throw new TransactionException("Trying to set a finish event twice. Old: " + this.finishEvent + ", new: " + finishEvent);
        }
    }

    void setFinishEvent(QueryEvent finishEvent) throws TransactionException {
        // MyIsam block
        if (!QueryEventType.COMMIT.equals(QueryInspector.getQueryEventType(finishEvent))) {
            throw new TransactionException("Can't set finishEvent for transaction to a wrong event type: " + finishEvent);
        }
        if (this.finishEvent == null) {
            setXid(FAKEXID);
            this.finishEvent = finishEvent;
            return;
        }
        if (isRewinded) {
            BinlogPositionInfo oldFinishEventPosition = new BinlogPositionInfo(((QueryEvent) this.finishEvent).getBinlogFilename(), this.finishEvent.getHeader().getPosition());
            BinlogPositionInfo newFinishEventPosition = new BinlogPositionInfo(finishEvent.getBinlogFilename(), finishEvent.getHeader().getPosition());
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

    BinlogEventV4 getFinishEvent() {
        return finishEvent;
    }

    boolean hasBeginEvent() {
        return (beginEvent != null);
    }
    boolean hasFinishEvent() {
        return (finishEvent != null);
    }

    void addEvent(BinlogEventV4 event) {
        events.add(event);
    }

    boolean hasEvents() {
        return (events.peek() != null);
    }

    public Queue<BinlogEventV4> getEvents() {
        return events;
    }

    long getEventsCounter() {
        return events.size();
    }

    void clearEvents() {
        events = new LinkedList<>();
    }

    void setBeginEventTimestamp(long timestamp) throws TransactionException {
        ((BinlogEventV4HeaderImpl) beginEvent.getHeader()).setTimestamp(timestamp);
    }

    void setBeginEventTimestampToFinishEvent() throws TransactionException {
        if (!hasFinishEvent()) {
            throw new TransactionException("Can't set timestamp to timestamp of finish event while no finishEvent exists");
        }
        setBeginEventTimestamp(finishEvent.getHeader().getTimestamp());
    }

    void setEventsTimestamp(long timestamp) throws TransactionException {
        for (BinlogEventV4 event : events) {
            ((BinlogEventV4HeaderImpl) event.getHeader()).setTimestamp(timestamp);
        }
    }

    void setEventsTimestampToFinishEvent() throws TransactionException {
        if (!hasFinishEvent()) {
            throw new TransactionException("Can't set timestamp to timestamp of finish event while no finishEvent exists");
        }
        setEventsTimestamp(finishEvent.getHeader().getTimestamp());
    }

    /**
     * Update table map cache.
     * @param event event
     */
    public void updateCache(TableMapEvent event) {
        LOGGER.debug("Updating cache. firstMapEventInTransaction: " + firstMapEventInTransaction + ", event: " + event);
        if (firstMapEventInTransaction == null) {
            firstMapEventInTransaction = event;
        }

        String tableName = event.getTableName().toString();

        tableID2Name.put(
                event.getTableId(),
                tableName
        );

        tableID2DBName.put(
                event.getTableId(),
                event.getDatabaseName().toString()
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

    public TableMapEvent getTableMapEvent(String tableName) {
        return currentTransactionTableMapEvents.get(tableName);
    }

    public TableMapEvent getFirstMapEventInTransaction() {
        return firstMapEventInTransaction;
    }

    boolean hasMappingInTransaction() {
        return firstMapEventInTransaction != null;
    }


    Map<String, TableMapEvent> getCurrentTransactionTableMapEvents() {
        return currentTransactionTableMapEvents;
    }

}
