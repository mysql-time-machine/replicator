package com.booking.replication.augmenter.cell;

/**
 * Extracted from: https://github.com/whitesock/open-replicator/blob/master/src/main/java/com/google/code/or/common/glossary/column/FloatColumn.java
 */
public class FloatCell implements Cell {

    private final float value;

    public FloatCell(float value) {
        this.value = value;
    }


    @Override
    public Float getValue() {
        return value;
    }
}
