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
        Configuration configuration = new Configuration();

        ReplicatorMetrics metrics = new ReplicatorMetrics(configuration);

        assertNotSame(metrics.getTotalsSnapshot(), metrics.getTotalsSnapshot());
    }
}
