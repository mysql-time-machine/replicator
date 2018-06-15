package com.booking.replication.augmenter.model;

import java.util.List;

@SuppressWarnings("unused")
public class QueryAugmentedEventData implements TableAugmentedEventData {
    private QueryAugmentedEventDataType queryType;
    private QueryAugmentedEventDataOperationType operationType;
    private AugmentedEventTable eventTable;
    private long threadId;
    private long executionTime;
    private int errorCode;
    private String sql;
    private List<AugmentedEventColumn> columnsBefore;
    private String createTableBefore;
    private List<AugmentedEventColumn> columnsAfter;
    private String createTableAfter;

    public QueryAugmentedEventData() {
    }

    public QueryAugmentedEventData(QueryAugmentedEventDataType queryType, QueryAugmentedEventDataOperationType operationType, AugmentedEventTable eventTable, long threadId, long executionTime, int errorCode, String sql, List<AugmentedEventColumn> columnsBefore, String createTableBefore, List<AugmentedEventColumn> columnsAfter, String createTableAfter) {
        this.queryType = queryType;
        this.operationType = operationType;
        this.eventTable = eventTable;
        this.threadId = threadId;
        this.executionTime = executionTime;
        this.errorCode = errorCode;
        this.sql = sql;
        this.columnsBefore = columnsBefore;
        this.createTableBefore = createTableBefore;
        this.columnsAfter = columnsAfter;
        this.createTableAfter = createTableAfter;
    }

    public QueryAugmentedEventDataType getQueryType() {
        return this.queryType;
    }

    public QueryAugmentedEventDataOperationType getOperationType() {
        return this.operationType;
    }

    @Override
    public AugmentedEventTable getEventTable() {
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

    public List<AugmentedEventColumn> getColumnsBefore() {
        return this.columnsBefore;
    }

    public String getCreateTableBefore() {
        return this.createTableBefore;
    }

    public List<AugmentedEventColumn> getColumnsAfter() {
        return this.columnsAfter;
    }

    public String getCreateTableAfter() {
        return this.createTableAfter;
    }
}
