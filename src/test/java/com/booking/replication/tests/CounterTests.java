package com.booking.replication.tests;

import com.booking.replication.metrics.Counter;
import org.junit.Test;

import java.math.BigInteger;

import static org.junit.Assert.*;

/**
 * Created by mdutikov on 5/25/2016.
 */
public class CounterTests {

    @Test
    public void makeSureMetricIncrementsWork() {
        Counter counter = new Counter("counter");

        assertEquals(BigInteger.ZERO, counter.getValue());

        counter.incrementBy(15);
        counter.increment();

        assertEquals(BigInteger.valueOf(16), counter.getValue());
    }
}
