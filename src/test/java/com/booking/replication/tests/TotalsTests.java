package com.booking.replication.tests;

import com.booking.replication.metrics.INameValue;
import com.booking.replication.metrics.Totals;
import org.junit.Test;

import java.math.BigInteger;
import java.util.HashMap;

import static org.junit.Assert.assertEquals;

/**
 * Created by mdutikov on 6/1/2016.
 */
public class TotalsTests {
    @Test
    public void makeSureGettingNamesAndValuesProducesCorrectResults() {
        Totals totals = new Totals();

        totals.getTotalHbaseRowsAffected().incrementBy(10);

        totals.getRowsForInsertProcessed().incrementBy(11);

        totals.getRowsForDeleteProcessed().incrementBy(12);

        totals.getRowsForUpdateProcessed().incrementBy(13);

        totals.getHbaseRowsAffected().incrementBy(14);

        totals.getInsertEvents().incrementBy(15);

        totals.getApplierTasksFailed().incrementBy(16);

        totals.getEventsSkipped().incrementBy(17);

        totals.getXidCounter().incrementBy(18);

        totals.getHeartBeatCounter().incrementBy(19);

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
        assertEquals(BigInteger.valueOf(14), namesToValues.get(totals.getHbaseRowsAffected().getName()));
        assertEquals(BigInteger.valueOf(15), namesToValues.get(totals.getInsertEvents().getName()));
        assertEquals(BigInteger.valueOf(16), namesToValues.get(totals.getApplierTasksFailed().getName()));
        assertEquals(BigInteger.valueOf(17), namesToValues.get(totals.getEventsSkipped().getName()));
        assertEquals(BigInteger.valueOf(18), namesToValues.get(totals.getXidCounter().getName()));
        assertEquals(BigInteger.valueOf(19), namesToValues.get(totals.getHeartBeatCounter().getName()));
        //TODO: more thorough?
    }
}
