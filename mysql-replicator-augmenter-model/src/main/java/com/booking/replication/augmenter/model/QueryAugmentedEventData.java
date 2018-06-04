package com.booking.replication.augmenter.model;

@SuppressWarnings("unused")
public class QueryAugmentedEventData implements AugmentedEventData {
    private QueryAugmentedEventDataType queryType;
    private long threadId;
    private long executionTime;
    private int errorCode;
    private String database;
    private String sql;

    public QueryAugmentedEventData() {
    }

    public QueryAugmentedEventData(QueryAugmentedEventDataType queryType, long threadId, long executionTime, int errorCode, String database, String sql) {
        this.queryType = queryType;
        this.threadId = threadId;
        this.executionTime = executionTime;
        this.errorCode = errorCode;
        this.database = database;
        this.sql = sql;
    }

    public QueryAugmentedEventDataType getQueryType() {
        return this.queryType;
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

    public String getSQL() {
        return this.sql;
    }
}