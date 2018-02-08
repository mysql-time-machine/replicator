package com.booking.replication.binlog.common.cell;

import com.booking.replication.binlog.common.Cell;
import java.math.BigDecimal;

/**
 * Extracted from https://github.com/whitesock/open-replicator/blob/master/src/main/java/com/google/code/or/common/glossary/column/DecimalColumn.java
 */
public class DecimalCell implements Cell {

    private final BigDecimal value;
    private final int precision;
    private final int scale;

    /**
     *
     */
    public DecimalCell(BigDecimal value, int precision, int scale) {
        this.value = value;
        this.scale = scale;
        this.precision = precision;
    }

    @Override
    public BigDecimal getValue() {
        return value;
    }

    public int getPrecision() {
        return precision;
    }

    public int getScale() {
        return scale;
    }
}
