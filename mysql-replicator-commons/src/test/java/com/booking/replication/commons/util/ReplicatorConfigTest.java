package com.booking.replication.commons.util;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ReplicatorConfigTest {

    @Test
    public void get() {
        final ReplicatorConfig<String, String> myMap = new ReplicatorConfig<>();
        myMap.put("one", "foo");
        final Object one = myMap.get("one");
        assertEquals("foo", one);
    }

    @Test(expected = NullPointerException.class)
    public void getWithConstraint() {
        final ReplicatorConfig<String, Object> myMap = new ReplicatorConfig<>();
        myMap.get("one", true);
    }

}