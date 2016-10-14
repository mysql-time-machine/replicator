package com.booking.replication.monitor;

import com.codahale.metrics.Counting;
import org.junit.Test;
import org.slf4j.Logger;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ReplicatorDoctorTest {

    @Test
    public void makeSureTheDocSaysWeAreFineIfTheCounterChangesBetweenTwoSamples() throws Exception {
        Counting counterMock = mock(Counting.class);

        when(counterMock.getCount()).thenReturn((long)0).thenReturn((long)500);

        ReplicatorDoctor doc = new ReplicatorDoctor(counterMock, "", mock(Logger.class));

        assertTrue(doc.makeHealthAssessment().isOk());
        assertTrue(doc.makeHealthAssessment().isOk());
    }

    @Test
    public void makeSureTheDocSaysWeAreNotFineIfTheCounterDoesntChangesBetweenTwoSamples() throws Exception {

        Counting counterMock = mock(Counting.class);

        when(counterMock.getCount()).thenReturn((long)0).thenReturn((long)500).thenReturn((long) 500);

        ReplicatorDoctor doc = new ReplicatorDoctor(counterMock, "", mock(Logger.class));

        // first reading is always OK, because there is no data for comparison
        assertTrue(doc.makeHealthAssessment().isOk());

        assertTrue(doc.makeHealthAssessment().isOk());
        assertFalse(doc.makeHealthAssessment().isOk());
    }
}