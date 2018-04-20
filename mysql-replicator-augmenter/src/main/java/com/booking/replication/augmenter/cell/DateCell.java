package com.booking.replication.augmenter.cell;

import java.sql.Date;

/**
 * Extracted from https://github.com/whitesock/open-replicator/blob/master/src/main/java/com/google/code/or/common/glossary/column/DateColumn.java
 */
public class DateCell implements Cell {

    private final Date value;

    /**
     *
     */
    public DateCell(Date value) {
        this.value = value;
    }

    @Override
    public Date getValue() {
        return value;
    }
}
