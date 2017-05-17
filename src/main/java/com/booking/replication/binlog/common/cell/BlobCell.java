package com.booking.replication.binlog.common.cell;

import com.booking.replication.binlog.common.Cell;

/**
 * Extracted from https://github.com/whitesock/open-replicator/blob/master/src/main/java/com/google/code/or/common/glossary/column/BlobColumn.java
 */
public class BlobCell implements Cell {

    private final byte[] value;

    public BlobCell(byte[] blob) {
        this.value = blob;
    }

    @Override
    public byte[] getValue() {
        return value;
    }
}
