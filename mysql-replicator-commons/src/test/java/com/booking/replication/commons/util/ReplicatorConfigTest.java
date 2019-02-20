package com.booking.replication.commons.util;

import org.junit.Test;

import java.util.HashMap;

import static org.junit.Assert.*;

public class ReplicatorConfigTest {
    @Test
    public void get() throws Exception {
        ReplicatorConfig myMap = new ReplicatorConfig();
        myMap.put("one", "foo");
        Object one = myMap.get("one");
        assertEquals("foo", one);
    }

    @Test(expected = NullPointerException.class)
    public void getWithContraint() throws Exception {
        ReplicatorConfig<String, Object> myMap = new ReplicatorConfig<>();
        Object one = myMap.get("one", true);
    }

}