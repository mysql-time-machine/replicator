package com.booking.replication.tests;

import com.booking.replication.Configuration;
import com.booking.replication.metrics.ReplicatorMetrics;
import com.booking.replication.metrics.Totals;
import org.junit.Test;

import java.util.ArrayList;

import static org.junit.Assert.assertNotSame;

/**
 * Created by mdutikov on 6/1/2016.
 */
public class ReplicatorMetricTests {
    @Test
    public void MakeSureSnapshotsAreDifferentObjectsEveryTime()
    {
        ReplicatorMetrics metrics = new ReplicatorMetrics(new ArrayList<String>());

        assertNotSame(metrics.getTotalsSnapshot(), metrics.getTotalsSnapshot());
    }
}
