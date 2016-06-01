package com.booking.replication.tests;

import com.booking.replication.metrics.Counter;
import com.booking.replication.metrics.CounterWithDependency;
import org.junit.Test;

import java.math.BigInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;

/**
 * Created by mdutikov on 6/1/2016.
 */

public class CounterWithDependencyTests {

    @Test
    public void makeSureMetricIncrementsWork() {
        Counter counter = new Counter("counter", BigInteger.valueOf(10));

        CounterWithDependency counterWithDependency = new CounterWithDependency(
                "dc", BigInteger.ZERO, counter);

        counterWithDependency.increment();

        assertEquals(BigInteger.valueOf(1), counterWithDependency.getValue());
        assertEquals(BigInteger.valueOf(11), counter.getValue());
    }

    @Test
    public void makeSureCopyCreatesIndependentObject() {

        Counter counter = new Counter("counter", BigInteger.valueOf(567));

        Counter copy = counter.copy();

        assertEquals(BigInteger.valueOf(567), copy.getValue());
        assertEquals(counter.getValue(), copy.getValue());

        assertNotSame(counter, copy);

        copy.increment();

        assertFalse(counter.getValue().equals(copy.getValue()));
    }
}
