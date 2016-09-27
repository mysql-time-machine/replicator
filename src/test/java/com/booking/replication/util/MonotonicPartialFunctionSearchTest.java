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

    @Test
    public void makeSureWeCanPullThruUndefinedFunctions()
    {
        String[] namesOfFiles = {"1", "2", "3", "4", "5", "6", "7", "8"};
        HashMap<String, String> fileNameToFirstGtidInThatFile = new HashMap<>();

        fileNameToFirstGtidInThatFile.put("1", "1000");
        fileNameToFirstGtidInThatFile.put("2", null);
        fileNameToFirstGtidInThatFile.put("3", null);
        fileNameToFirstGtidInThatFile.put("4", "2000");
        fileNameToFirstGtidInThatFile.put("5", null);
        fileNameToFirstGtidInThatFile.put("6", "4000");
        fileNameToFirstGtidInThatFile.put("7", null);
        fileNameToFirstGtidInThatFile.put("8", "6000");

        MonotonicPartialFunctionSearch<String, String> f = new MonotonicPartialFunctionSearch<>(
                x -> fileNameToFirstGtidInThatFile.get(x), namesOfFiles);

        assertEquals("4", f.reverseGLB("3000"));
    }

    @Test
    public void makeSureWeProduceNoResultsIfFunctionValueIsNotDefinedForAnything()
    {
        String[] namesOfFiles = {"1", "2", "3", "4", "5", "6", "7", "8"};

        MonotonicPartialFunctionSearch<String, String> f = new MonotonicPartialFunctionSearch<>(
                x -> null, namesOfFiles);

        assertEquals(null, f.reverseGLB("3000"));
    }
}