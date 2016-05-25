package com.booking.replication.tests;

import com.booking.replication.metrics.SetOfMetrics;
import org.junit.Test;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Created by mdutikov on 5/25/2016.
 */
public class SetOfMetricsTests {

    @Test
    public void makeSureUninitializedMetricsReadZeroes() throws Exception {

        SetOfMetrics setOfMetrics = new SetOfMetrics();

        assertEquals(BigInteger.ZERO, setOfMetrics.getMetricValue(123));
    }

    @Test
    public void makeSureMetricIncrementsWork() throws Exception {

        SetOfMetrics setOfMetrics = new SetOfMetrics();

        setOfMetrics.incrementMetric(123);
        setOfMetrics.incrementMetric(123);
        setOfMetrics.incrementMetric(123);

        assertEquals(BigInteger.valueOf(3), setOfMetrics.getMetricValue(123));
    }

    @Test
    public void makeSureMetricIncrementsOfMoreThanOneWork() throws Exception {

        SetOfMetrics setOfMetrics = new SetOfMetrics();

        setOfMetrics.incrementMetric(123, 3);

        setOfMetrics.incrementMetric(156);
        setOfMetrics.incrementMetric(156);
        setOfMetrics.incrementMetric(156);

        assertEquals(BigInteger.valueOf(3), setOfMetrics.getMetricValue(123));
        assertEquals(setOfMetrics.getMetricValue(156), setOfMetrics.getMetricValue(123));
    }

    private class MetricIncrementingThread implements Runnable
    {
        private final SetOfMetrics setOfMetrics;
        private final int metricToIncrement;
        private final CountDownLatch threadSynchronizationLatch;

        MetricIncrementingThread(SetOfMetrics setOfMetrics, int metricToIncrement, CountDownLatch threadSynchronizationLatch)
        {
            assertNotNull(setOfMetrics);
            assertNotNull(threadSynchronizationLatch);

            this.metricToIncrement = metricToIncrement;
            this.threadSynchronizationLatch = threadSynchronizationLatch;
            this.setOfMetrics = setOfMetrics;
        }

        @Override
        public void run() {
            threadSynchronizationLatch.countDown();
            try {
                threadSynchronizationLatch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            setOfMetrics.incrementMetric(metricToIncrement);
        }
    }

    @Test
    public void makeSureMetricIncrementsWorkInThreadSafeManner() throws Exception {

        int metricId = 56;
        int numberOfThreads = 100;

        SetOfMetrics setOfMetrics = new SetOfMetrics();

        List<Thread> threadList = new ArrayList<Thread>();

        CountDownLatch latch = new CountDownLatch(numberOfThreads);

        for (int i = 0; i < numberOfThreads; i++)
        {
            threadList.add(new Thread(new MetricIncrementingThread(setOfMetrics, metricId, latch)));
        }

        for (int i = 0; i < threadList.size(); i++) {
            threadList.get(i).start();
        }

        for (int i = 0; i < threadList.size(); i++) {
            threadList.get(i).join();
        }

        assertEquals(BigInteger.valueOf(numberOfThreads), setOfMetrics.getMetricValue(metricId));
    }
}
