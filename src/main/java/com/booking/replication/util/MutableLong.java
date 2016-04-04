package com.booking.replication.util;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by bosko on 12/24/15.
 */
public class MutableLong {

    private AtomicLong value;

    public MutableLong() {
        value = new AtomicLong(0L);
    }

    public MutableLong(long val) {
        value = new AtomicLong(val);
    }

    public void increment () {
        value.incrementAndGet();
    }

    public long getValue() {
        return value.get();
    }

    public void setValue(Long newValue) { value.set(newValue); }

    public void addValue(Long valueToAdd) {
        value.addAndGet(valueToAdd);
    }
}
