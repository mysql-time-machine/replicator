package com.booking.replication.pipeline;

import com.booking.replication.schema.exception.TableMapException;
import com.google.code.or.binlog.impl.event.TableMapEvent;
import com.google.common.base.Joiner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by bosko on 11/10/15.
 */
public class CurrentTransactionMetadata {

    private Map<Long,String> tableID2Name = new HashMap<>();
    private Map<Long, String> tableID2DBName = new HashMap<>();

    private TableMapEvent firstMapEventInTransaction = null;

    private final Map<String, TableMapEvent> currentTransactionTableMapEvents = new HashMap<>();

    private static final Logger LOGGER = LoggerFactory.getLogger(CurrentTransactionMetadata.class);

    /**
     * Update table map cache.
     */
    public void updateCache(TableMapEvent event) {

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
     */
    public String getTableNameFromID(Long tableID) throws TableMapException {
        if (! tableID2DBName.containsKey(tableID)) {
            LOGGER.error(String.format(
                    "Table ID not known. Known tables and ids are: %s",
                    Joiner.on(" ").join(tableID2DBName.keySet(), " ")));
            throw new TableMapException("Table ID not present in CurrentTransactionMetadata!");
        }

        return tableID2Name.get(tableID);
    }

    /**
     * Map table id to schema name.
     */
    public String getDBNameFromTableID(Long tableID) throws TableMapException {
        String dbName = tableID2DBName.get(tableID);

        if (dbName == null) {
            throw new TableMapException("Table ID not present in CurrentTransactionMetadata!");
        } else {
            return dbName;
        }
    }

    public TableMapEvent getTableMapEvent(String tableName) {
        return currentTransactionTableMapEvents.get(tableName);
    }

    public TableMapEvent getFirstMapEventInTransaction() {
        return firstMapEventInTransaction;
    }


    public Map<String, TableMapEvent> getCurrentTransactionTableMapEvents() {
        return currentTransactionTableMapEvents;
    }

}
