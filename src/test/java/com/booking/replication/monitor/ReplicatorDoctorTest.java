package com.booking.replication.monitor;

import com.codahale.metrics.Counting;
import org.jruby.RubyProcess;
import org.junit.Test;
import org.slf4j.Logger;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ReplicatorDoctorTest {

    @Test
    public void makeSureTheDocSaysWeAreFineIfTheCountersChangeBetweenTwoSamples() throws Exception {
        Counting externalWorkCounter = mock(Counting.class);
        Counting interestingEventsObservedCounter = mock(Counting.class);

        when(externalWorkCounter.getCount()).thenReturn((long) 0).thenReturn((long)500);
        when(interestingEventsObservedCounter.getCount()).thenReturn((long)500).thenReturn((long)1500);

        ReplicatorDoctor doc = new ReplicatorDoctor(externalWorkCounter, "", mock(Logger.class), interestingEventsObservedCounter);

        assertTrue(doc.makeHealthAssessment().isOk());
        assertTrue(doc.makeHealthAssessment().isOk());
    }

    @Test
    public void makeSureTheDocSaysWeAreNotFineIfTheCountersDontChangeBetweenTwoSamples() throws Exception {

        Counting externalWorkCounter = mock(Counting.class);
        Counting interestingEventsObservedCounter = mock(Counting.class);

        when(externalWorkCounter.getCount()).thenReturn((long)0).thenReturn((long)500).thenReturn((long) 500);
        when(interestingEventsObservedCounter.getCount()).thenReturn((long)0).thenReturn((long)500).thenReturn((long)1500);

        ReplicatorDoctor doc = new ReplicatorDoctor(externalWorkCounter, "", mock(Logger.class), interestingEventsObservedCounter);

        // first reading is always OK, because there is no data for comparison
        assertTrue(doc.makeHealthAssessment().isOk());

        assertTrue(doc.makeHealthAssessment().isOk());
        assertFalse(doc.makeHealthAssessment().isOk());
    }

    @Test
    public void makeSureTheDocSaysWeAreFineIfWeHaventSeenAnythingInterestingAndHaventPushedNothingOutsideBetweenTwoSamples() throws Exception {

        Counting externalWorkCounter = mock(Counting.class);
        Counting interestingEventsObservedCounter = mock(Counting.class);

        when(externalWorkCounter.getCount()).thenReturn((long) 0).thenReturn((long)500).thenReturn((long) 500);
        when(interestingEventsObservedCounter.getCount()).thenReturn((long)0).thenReturn((long)500).thenReturn((long)500);

        ReplicatorDoctor doc = new ReplicatorDoctor(externalWorkCounter, "", mock(Logger.class), interestingEventsObservedCounter);

        // first reading is always OK, because there is no data for comparison
        assertTrue(doc.makeHealthAssessment().isOk());

        assertTrue(doc.makeHealthAssessment().isOk());
        assertTrue(doc.makeHealthAssessment().isOk());
    }
}