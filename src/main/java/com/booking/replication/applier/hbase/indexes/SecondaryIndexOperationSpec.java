package com.booking.replication.applier.hbase.indexes;

/**
 * Created by bosko on 1/5/17.
 */
public class SecondaryIndexOperationSpec {

    private String operationType;

    private String secondaryIndexHBaseRowKeyBefore;

    private String secondaryIndexHBaseRowKeyAfter;

    public SecondaryIndexOperationSpec(
            String operationType,
            String secondaryIndexHBaseRowKeyBefore,
            String secondaryIndexHBaseRowKeyAfter) {
        this.operationType = operationType;
        this.secondaryIndexHBaseRowKeyBefore = secondaryIndexHBaseRowKeyBefore;
        this.secondaryIndexHBaseRowKeyAfter = secondaryIndexHBaseRowKeyAfter;
    }

    public String getOperationType() {
        return operationType;
    }

    public String getSecondaryIndexHBaseRowKeyBefore() {
        return secondaryIndexHBaseRowKeyBefore;
    }

    public String getSecondaryIndexHBaseRowKeyAfter() {
        return secondaryIndexHBaseRowKeyAfter;
    }
}
