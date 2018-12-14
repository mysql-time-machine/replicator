package com.booking.replication.augmenter.model.event;

import com.booking.replication.augmenter.model.schema.SchemaSnapshot;
import com.booking.replication.augmenter.model.schema.TableSchema;
import com.booking.replication.augmenter.model.schema.FullTableName;

@SuppressWarnings("unused")
public class QueryAugmentedEventData implements TableAugmentedEventData {
    private QueryAugmentedEventDataType queryType;
    private QueryAugmentedEventDataOperationType operationType;
    private FullTableName eventTable;
    private long threadId;
    private long executionTime;
    private int errorCode;
    private String sql;
    private TableSchema before;
    private TableSchema after;
    private boolean isCompatibleSchemaChange = true;
    private boolean isDDL = false;
    private SchemaSnapshot schemaSnapshotOnDDL = null;

    public QueryAugmentedEventData() {
    }

    public QueryAugmentedEventData(QueryAugmentedEventDataType queryType, QueryAugmentedEventDataOperationType operationType, FullTableName eventTable, long threadId, long executionTime, int errorCode, String sql, TableSchema before, TableSchema after) {
        this.queryType = queryType;
        this.operationType = operationType;
        this.eventTable = eventTable;
        this.threadId = threadId;
        this.executionTime = executionTime;
        this.errorCode = errorCode;
        this.sql = sql;
        this.before = before;
        this.after = after;
    }

    public QueryAugmentedEventDataType getQueryType() {
        return this.queryType;
    }

    public QueryAugmentedEventDataOperationType getOperationType() {
        return this.operationType;
    }

    @Override
    public FullTableName getEventTable() {
        return this.eventTable;
    }

    public long getThreadId() {
        return this.threadId;
    }

    public long getExecutionTime() {
        return this.executionTime;
    }

    public int getErrorCode() {
        return this.errorCode;
    }

    public String getSQL() {
        return this.sql;
    }

    public TableSchema getBefore() {
        return this.before;
    }

    public TableSchema getAfter() {
        return this.after;
    }

    public boolean isDDL() {
        return isDDL;
    }

    public void setDDL(boolean DDL) {
        isDDL = DDL;
    }

    public void setSchemaCompatibilityFlag(boolean isCompatibleSchemaChange){
        this.isCompatibleSchemaChange = isCompatibleSchemaChange;
    }

    public boolean getIsCompatibleSchemaChange(){
        return this.isCompatibleSchemaChange;
    }

    public SchemaSnapshot getSchemaSnapshotOnDDL() {
        return schemaSnapshotOnDDL;
    }

    public void setSchemaSnapshotOnDDL(SchemaSnapshot schemaSnapshotOnDDL) {
        this.schemaSnapshotOnDDL = schemaSnapshotOnDDL;
    }
}
