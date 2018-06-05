package com.booking.replication.binlog.event.impl;

import com.booking.replication.binlog.event.impl.BinlogEvent;
import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.google.code.or.binlog.impl.event.TableMapEvent;
import com.google.code.or.common.glossary.column.StringColumn;

/**
 * Created by bosko on 5/22/17.
 */
public class BinlogEventTableMap extends BinlogEvent {

    public BinlogEventTableMap(Object event) throws Exception {
        super(event);
    }

    public long getTableId() {
        if (this.USING_DEPRECATED_PARSER) {
            return  ((TableMapEvent) this.getBinlogEventV4()).getTableId();
        }
        else {
            return ((TableMapEventData) this.getBinlogConnectorEvent().getData()).getTableId();
        }
    }

    public String getTableName() {
        if (this.USING_DEPRECATED_PARSER) {
            return  ((TableMapEvent) this.getBinlogEventV4()).getTableName().toString();
        }
        else {
            return ((TableMapEventData) this.getBinlogConnectorEvent().getData()).getTable();
        }
    }

    public void setTableName(String tableName) {
        if (this.USING_DEPRECATED_PARSER) {
            ((TableMapEvent) this.getBinlogEventV4()).setTableName(
                    StringColumn.valueOf(tableName.getBytes())
            );
        }
        else {
            ((TableMapEventData) this.getBinlogConnectorEvent().getData()).setTable(tableName);
        }
    }

    public String getDatabaseName() {
        if (this.USING_DEPRECATED_PARSER) {
            return  ((TableMapEvent) this.getBinlogEventV4()).getDatabaseName().toString();
        }
        else {
            return ((TableMapEventData) this.getBinlogConnectorEvent().getData()).getDatabase();
        }
    }

    public void setDatabaseName(String databaseName) {
        if (this.USING_DEPRECATED_PARSER) {
            ((TableMapEvent) this.getBinlogEventV4()).setDatabaseName(
                    StringColumn.valueOf(databaseName.getBytes())
            );
        }
        else {
            ((TableMapEventData) this.getBinlogConnectorEvent().getData()).setDatabase(databaseName);
        }
    }
}
