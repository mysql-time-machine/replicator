package com.booking.replication.metrics;

import java.math.BigInteger;
import java.util.HashMap;

/*
   A thread-safe set of metric counters.

   TODO: fix the overflows on downstream conversion to long - use BigInteger everywhere?
 */
public class SetOfMetrics
{
    private final HashMap<Integer, BigInteger> metricToCounter = new HashMap<>();
    private final Object criticalSection = new Object();

    public BigInteger getMetricValue(int metricId)
    {
        synchronized (criticalSection)
        {
            BigInteger value = metricToCounter.get(metricId);

            if (value == null)
            {
                return BigInteger.ZERO;
            }

            return value;
        }
    }

    public BigInteger incrementMetric(int metricId)
    {
        return incrementMetric(metricId, 1);
    }

    /*TODO: incrementBy can be negative, do we want this? Rename the method if so*/
    public BigInteger incrementMetric(int metricId, long incrementBy)
    {
        synchronized (criticalSection)
        {
            if (metricToCounter.containsKey(metricId))
            {
                BigInteger value = metricToCounter.get(metricId);
                value = value.add(BigInteger.valueOf(incrementBy));
                metricToCounter.put(metricId, value);
                return value;
            }
            else
            {
                metricToCounter.put(metricId, BigInteger.valueOf(incrementBy));
                return BigInteger.valueOf(incrementBy);
            }
        }
    }
}