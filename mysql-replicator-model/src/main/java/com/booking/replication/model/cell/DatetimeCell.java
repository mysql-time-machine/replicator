package com.booking.replication.binlog.common.cell;

import com.booking.replication.binlog.common.Cell;

import java.util.Date;

/**
 * Extracted from https://github.com/whitesock/open-replicator/blob/master/src/main/java/com/google/code/or/common/glossary/column/DatetimeColumn.java
 */
public class DatetimeCell implements Cell {

    private final java.util.Date value;

    public DatetimeCell(java.util.Date value) {
        this.value = value;
    }

    @Override
    public Date getValue() {
        return value;
    }
}
