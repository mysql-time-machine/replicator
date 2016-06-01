package com.booking.replication.metrics;

import java.math.BigInteger;

/**
 * Created by mdutikov on 5/30/2016.
 */
public class Metric implements INameValue {
    private final String name;
    private BigInteger number;

    public Metric(String name)
    {
        this.name = name;
        this.number = BigInteger.ZERO;
    }

    public BigInteger getValue()
    {
        return number;
    }

    public Metric copy() {
        Metric copy = new Metric(name);
        copy.setValue(number);

        return copy;
    }

    public void setValue(BigInteger v)
    {
        // TODO: args
        number = v;
    }

    public String getName() {
        return name;
    }
}
