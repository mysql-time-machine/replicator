package com.booking.replication.binlog.common.cell;

import com.booking.replication.binlog.common.Cell;

/**
 * Created by bosko on 6/21/17.
 */

/**
 * extracted from: https://github.com/whitesock/open-replicator/blob/master/src/main/java/com/google/code/or/common/glossary/column/SetColumn.java
 */
public class SetCell implements Cell {

    private final long value;

    public SetCell(long value) {
        this.value = value;
    }

    @Override
    public Long getValue() {
        return value;
    }
}
