package com.booking.replication.tests;

import com.booking.replication.metrics.INameValue;
import com.booking.replication.metrics.RowTotals;
import org.junit.Test;

import java.math.BigInteger;
import java.util.HashMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

/**
 * Created by mdutikov on 6/1/2016.
 */
public class RowTotalsTests {

    @Test
    public void makeSureGettingNamesAndValuesProducesCorrectResults() {
        RowTotals totals = new RowTotals();

        totals.getTotalHbaseRowsAffected().incrementBy(10);

        totals.getRowsForInsertProcessed().incrementBy(11);

        totals.getRowsForDeleteProcessed().incrementBy(12);

        totals.getRowsForUpdateProcessed().incrementBy(13);

        totals.getTotalRowsProcessed().incrementBy(14);

        INameValue[] namesAndValues = totals.getAllNamesAndValues();
        HashMap<String, BigInteger> namesToValues = new HashMap<>();

        for (int i = 0; i < namesAndValues.length; i++)
        {
            namesToValues.put(namesAndValues[i].getName(), namesAndValues[i].getValue());
        }

        assertEquals(BigInteger.valueOf(10), namesToValues.get(totals.getTotalHbaseRowsAffected().getName()));
        assertEquals(BigInteger.valueOf(11), namesToValues.get(totals.getRowsForInsertProcessed().getName()));
        assertEquals(BigInteger.valueOf(12), namesToValues.get(totals.getRowsForDeleteProcessed().getName()));
        assertEquals(BigInteger.valueOf(13), namesToValues.get(totals.getRowsForUpdateProcessed().getName()));
        assertEquals(BigInteger.valueOf(14), namesToValues.get(totals.getTotalRowsProcessed().getName()));
        assertEquals(5, namesAndValues.length);
    }
}
