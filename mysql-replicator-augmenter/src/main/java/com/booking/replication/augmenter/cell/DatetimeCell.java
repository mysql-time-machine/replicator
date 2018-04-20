package com.booking.replication.augmenter.cell;

import java.util.Date;

/**
 * Extracted from https://github.com/whitesock/open-replicator/blob/master/src/main/java/com/google/code/or/common/glossary/column/DatetimeColumn.java
 */
public class DatetimeCell implements Cell {

    private final Date value;

    public DatetimeCell(Date value) {
        this.value = value;
    }

    @Override
    public Date getValue() {
        return value;
    }
}
