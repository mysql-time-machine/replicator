package com.booking.replication.binlog.common.cell;

import com.booking.replication.binlog.common.Cell;

/**
 * Extracted from https://github.com/whitesock/open-replicator/blob/master/src/main/java/com/google/code/or/common/glossary/column/DoubleColumn.java
 */
public class DoubleCell implements Cell {

    private final double value;

    public DoubleCell(double value) {
        this.value = value;
    }

    @Override
    public Double getValue() {
        return value;
    }
}
