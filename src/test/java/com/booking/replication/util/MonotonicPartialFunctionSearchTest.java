package com.booking.replication.util;


import org.junit.Test;

import java.util.Random;

import static org.junit.Assert.assertEquals;

import static org.junit.Assert.assertFalse;

import static org.junit.Assert.assertNull;


public class MonotonicPartialFunctionSearchTest {

    @Test
    public void makeSureWeCanFindTheValueInTheObviousCase()
    {
        String[] files = {"001:5000", "002:5700", "003:6700"};

        assertEquals("002:5700", MonotonicPartialFunctionSearch.preimageGLB( x -> x.split(":")[1], "6699", files ));
    }

    @Test
    public void makeSureWeDontFindAnythingIfThereIsNoEntryLessThanTheValueWeSeek()
    {
        String[] files = {"001:5000", "002:45000"};

        assertNull(MonotonicPartialFunctionSearch.preimageGLB( x -> x.split(":")[1], "2000", files ));
    }

    @Test
    public void makeSureWeCanPullThruUndefinedFunctions()
    {
        String[] values = {"1000", null, null, "2000", null, "4000", null, "6000"};

        MonotonicPartialFunctionSearch<String> f = new MonotonicPartialFunctionSearch<>(
                x -> values[x]);

        assertEquals(Integer.valueOf(3), f.preimageGLB("3000",0,values.length-1));
    }

    @Test
    public void makeSureWeProduceNoResultsIfFunctionValueIsNotDefinedForAnything()
    {
        String[] files = {"1", "2", "3", "4", "5", "6", "7", "8"};


        assertNull(MonotonicPartialFunctionSearch.preimageGLB( x ->null, "3000", files ));
        assertNull(MonotonicPartialFunctionSearch.preimageGLB( x ->null, "3", files ));
    }

    @Test
    public void makeSureWeProduceResultIfDomainHasOnlyOneElement()
    {
        String[] files = {"001:050"};

        assertEquals("001:050", MonotonicPartialFunctionSearch.preimageGLB( x -> x.split(":")[1], "1000", files ));
    }

    @Test
    public void makeSureWeCallAFunctionNoMoreThanOnceForGivenDomainValueWhileSearching()
    {
        Integer[] namesOfFiles = new Integer[100];

        for (int i = 0; i < 100; i++) {
            namesOfFiles[i] = i;
        }

        Boolean[] domainValuesHitMap = new Boolean[100];

        for (int i = 0; i < 100; i++) {
            domainValuesHitMap[i] = false;
        }

        MonotonicPartialFunctionSearch<Integer> f = new MonotonicPartialFunctionSearch<>(
                    x ->
                    {
                        assertFalse(domainValuesHitMap[x]);
                        domainValuesHitMap[x] = true;

                        return x;
                    }
                );

        assertEquals(Integer.valueOf(2), f.preimageGLB(2,0,99));
    }

    @Test
    public void randomizedTest(){

        for (int i = 0; i < 100; i++){

            Random r = new Random();

            int size = r.nextInt(50);

            final Integer[] values = new Integer[size];

            for (int j = 0; j < size; j++){
                if (r.nextBoolean()) values[j] = j;
            }

            for (int c = -1; c<size; c++){

                final int[] callCount = new int[size];

                Integer a1 = MonotonicPartialFunctionSearch.preimageGLB(x-> {
                        callCount[x]++;
                        if (callCount[x]>1) {
                            throw new AssertionError("Too many calls to " +x);
                        }
                        return values[x];
                    }, c, 0, values.length-1 );

                Integer a2 = null;
                for (int v = c; v >=0;v--){
                    if (values[v] != null) {
                        a2 = values[v];
                        break;
                    }
                }

                assertEquals(a2,a1);
            }
        }
    }
}