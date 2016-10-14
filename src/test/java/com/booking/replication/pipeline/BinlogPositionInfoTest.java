package com.booking.replication.pipeline;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class BinlogPositionInfoTest {

    @Test
    public void testBinlogPositionComparisonIfTheyAreOnlyDifferentInOffset() throws Exception {

        BinlogPositionInfo position1 = new BinlogPositionInfo("host", 1, "binlog.008720", 245);
        BinlogPositionInfo position2 = new BinlogPositionInfo("host", 1, "binlog.008720", 2056);

        assertTrue(position2.greaterThan(position1));
        assertFalse(position1.greaterThan(position2));
    }

    @Test
    public void testBinlogPositionComparisonIfTheyAreInDifferentFiles() throws Exception {

        BinlogPositionInfo position1 = new BinlogPositionInfo("host", 1, "binlog.000190", 9885745);
        BinlogPositionInfo position2 = new BinlogPositionInfo("host", 1, "binlog.008720", 2);

        assertTrue(position2.greaterThan(position1));
        assertFalse(position1.greaterThan(position2));
    }

    @Test(expected=Exception.class)
    public void testBinlogPositionComparisonThrowsIfWeAreComparingDifferentHostBinlogFiles() throws Exception {

        BinlogPositionInfo position1 = new BinlogPositionInfo("host25", 1, "binlog.000190", 9885745);
        BinlogPositionInfo position2 = new BinlogPositionInfo("host1", 1, "binlog.008720", 2);

        position2.greaterThan(position1);
    }
}