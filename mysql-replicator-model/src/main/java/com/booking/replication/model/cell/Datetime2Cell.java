package com.booking.replication.model.cell;

import java.util.Date;

/**
 * Extracted from https://github.com/whitesock/open-replicator/blob/master/src/main/java/com/google/code/or/common/glossary/column/Datetime2Column.java
 */
public class Datetime2Cell implements Cell {

    private final Date value;

    public Datetime2Cell(Date value) {
        this.value = value;
    }

    @Override
    public Date getValue() {
        return value;
    }
}
