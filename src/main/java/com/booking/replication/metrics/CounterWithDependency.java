package com.booking.replication.metrics;

import java.math.BigInteger;

/**
 *
 * Created by mdutikov on 5/27/2016.
 */
public class CounterWithDependency implements ICounter {

    private Counter counter;
    private final ICounter counterToUpdateWhenThisCounterIncrements;

    public CounterWithDependency(ICounter counterToUpdateWhenThisCounterIncrements)
    {
        this(counterToUpdateWhenThisCounterIncrements.getName(),
                BigInteger.ZERO,
                counterToUpdateWhenThisCounterIncrements);
    }

    public CounterWithDependency(String name, BigInteger value, ICounter counterToUpdateWhenThisCounterIncrements)
    {
        if (counterToUpdateWhenThisCounterIncrements == null)
            throw new IllegalArgumentException("countersToUpdateWhenThisCounterIncrements");

        counter = new Counter(name, value);

        this.counterToUpdateWhenThisCounterIncrements = counterToUpdateWhenThisCounterIncrements;
    }

    @Override
    public BigInteger getValue() {
        return counter.getValue();
    }

    @Override
    public void increment() {
        counter.increment();

        counterToUpdateWhenThisCounterIncrements.increment();
    }

    @Override
    public void incrementBy(long value) {
        counter.incrementBy(value);

        counterToUpdateWhenThisCounterIncrements.incrementBy(value);
    }

    @Override
    public ICounter copy() {
        // TODO: this doesn't make sense at all, revisit
        return new CounterWithDependency(
                this.counter.getName(), this.counter.getValue(), counterToUpdateWhenThisCounterIncrements);
    }

    @Override
    public String getName() {
        return counter.getName();
    }
}
