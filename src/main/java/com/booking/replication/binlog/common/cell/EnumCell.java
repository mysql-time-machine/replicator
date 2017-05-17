package com.booking.replication.binlog.common.cell;

import com.booking.replication.binlog.common.Cell;

/**
 * Extracted from https://github.com/whitesock/open-replicator/blob/master/src/main/java/com/google/code/or/common/glossary/column/EnumColumn.java
 */
public class EnumCell implements Cell {

    private final int value;

    public EnumCell(int value) {
        this.value = value;
    }

    @Override
    public Integer getValue() {
        return value;
    }
}
