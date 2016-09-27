package com.booking.replication.util;

import com.booking.replication.sql.QueryInspector;
import org.junit.Test;

import java.util.HashMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class MonotonicPartialFunctionSearchTest {

    @Test
    public void makeSureWeCanFindTheValueInTheObviousCase()
    {
        String[] namesOfFiles = {"001", "002", "003"};
        HashMap<String, String> fileNameToFirstGtidInThatFile = new HashMap<>();

        fileNameToFirstGtidInThatFile.put("001", "5000");
        fileNameToFirstGtidInThatFile.put("002", "5700");
        fileNameToFirstGtidInThatFile.put("003", "6700");

        MonotonicPartialFunctionSearch<String, String> f = new MonotonicPartialFunctionSearch<>(
                x -> fileNameToFirstGtidInThatFile.get(x), namesOfFiles);

        assertEquals("002", f.reverseGLB("6699"));
    }

    @Test
    public void makeSureWeDontFindAnythingIfThereIsNoEntryLessThanTheValueWeSeek()
    {
        String[] namesOfFiles = {"001", "002"};
        HashMap<String, String> fileNameToFirstGtidInThatFile = new HashMap<>();

        fileNameToFirstGtidInThatFile.put("001", "5000");
        fileNameToFirstGtidInThatFile.put("002", "45000");

        MonotonicPartialFunctionSearch<String, String> f = new MonotonicPartialFunctionSearch<>(
                x -> fileNameToFirstGtidInThatFile.get(x), namesOfFiles);

        assertEquals(null, f.reverseGLB("2000"));
    }
}