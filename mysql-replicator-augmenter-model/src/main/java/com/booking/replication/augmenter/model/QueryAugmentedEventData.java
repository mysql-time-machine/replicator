package com.booking.replication.augmenter.model;

@SuppressWarnings("unused")
public class QueryAugmentedEventData implements TableAugmentedEventData {
    private QueryAugmentedEventDataType queryType;
    private QueryAugmentedEventDataOperationType operationType;
    private AugmentedEventTable eventTable;
    private long threadId;
    private long executionTime;
    private int errorCode;
    private String sql;
    private AugmentedEventSchema before;
    private AugmentedEventSchema after;

    public QueryAugmentedEventData() {
    }

    public QueryAugmentedEventData(QueryAugmentedEventDataType queryType, QueryAugmentedEventDataOperationType operationType, AugmentedEventTable eventTable, long threadId, long executionTime, int errorCode, String sql, AugmentedEventSchema before, AugmentedEventSchema after) {
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

    public AugmentedEventSchema getBefore() {
        return this.before;
    }

    public AugmentedEventSchema getAfter() {
        return this.after;
    }
}
