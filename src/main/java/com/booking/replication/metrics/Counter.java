package com.booking.replication.metrics;

import java.math.BigInteger;

/**
 * Created by mdutikov on 5/27/2016.
 */
public class Counter implements ICounter {
    private final String name;
    private BigInteger value;

    public Counter(String name)
    {
        this(name, BigInteger.ZERO);
    }

    public Counter(String name, BigInteger startingValue)
    {
        // TODO: args
        this.name = name;
        value = startingValue;
    }

    @Override
    public BigInteger getValue()
    {
        return value;
    }

    public Counter copy() {
        return new Counter(name, value);
    }

    @Override
    public void increment()
    {
        value = value.add(BigInteger.ONE);
    }

    @Override
    public void incrementBy(long v) {
        value = value.add(BigInteger.valueOf(v));
    }

    @Override
    public String getName() {
        return name;
    }
}
