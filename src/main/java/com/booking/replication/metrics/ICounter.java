package com.booking.replication.metrics;

import java.math.BigInteger;

/**
 * Created by mdutikov on 5/27/2016.
 */
public interface ICounter extends INameValue {

    void increment();

    void incrementBy(long value);

    ICounter copy();

}
