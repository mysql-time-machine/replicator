package com.booking.replication.augmenter.model;

@SuppressWarnings("unused")
public class QueryAugmentedEventData implements AugmentedEventData {
    private QueryAugmentedEventDataType queryType;
    private QueryAugmentedEventDataOperationType operationType;
    private long threadId;
    private long executionTime;
    private int errorCode;
    private String database;
    private String table;
    private String sql;
    private String createTableBefore;
    private String createTableAfter;

    public QueryAugmentedEventData() {
    }

    public QueryAugmentedEventData(QueryAugmentedEventDataType queryType, QueryAugmentedEventDataOperationType operationType, long threadId, long executionTime, int errorCode, String database, String table, String sql, String createTableBefore, String createTableAfter) {
        this.queryType = queryType;
        this.operationType = operationType;
        this.threadId = threadId;
        this.executionTime = executionTime;
        this.errorCode = errorCode;
        this.database = database;
        this.table = table;
        this.sql = sql;
        this.createTableBefore = createTableBefore;
        this.createTableAfter = createTableAfter;
    }

    public QueryAugmentedEventDataType getQueryType() {
        return this.queryType;
    }

    public QueryAugmentedEventDataOperationType getOperationType() {
        return this.operationType;
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

    public String getDatabase() {
        return this.database;
    }

    public String getTable() {
        return this.table;
    }

    public String getSQL() {
        return this.sql;
    }

    public String getCreateTableBefore() {
        return this.createTableBefore;
    }

    public String getCreateTableAfter() {
        return this.createTableAfter;
    }
}
