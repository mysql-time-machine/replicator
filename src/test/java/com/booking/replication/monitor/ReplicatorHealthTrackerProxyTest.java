package com.booking.replication.monitor;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;

public class ReplicatorHealthTrackerProxyTest {

    @Test
    public void makeSureTheProxyReturnsNormalHealthAssessmentsByDefaultAfterCreation() throws Exception {

        ReplicatorHealthTrackerProxy proxy = new ReplicatorHealthTrackerProxy();

        assertTrue(proxy.getLastHealthAssessment().isOk());
        assertTrue(proxy.getLastHealthAssessment().isOk());
        assertTrue(proxy.getLastHealthAssessment().isOk());
        assertTrue(proxy.getLastHealthAssessment().isOk());
        assertTrue(proxy.getLastHealthAssessment().isOk());
    }

    @Test(expected=IllegalStateException.class)
    public void makeSureYouCannotStartAProxyWithoutSettingUnderlyingImplementationFirst() throws Exception {

        ReplicatorHealthTrackerProxy proxy = new ReplicatorHealthTrackerProxy();

        proxy.start();
    }

    @Test(expected=IllegalStateException.class)
    public void makeSureYouCannotStopAProxyWithoutSettingUnderlyingImplementationFirst() throws Exception {

        ReplicatorHealthTrackerProxy proxy = new ReplicatorHealthTrackerProxy();

        proxy.stop();
    }

    @Test
    public void makeSureStartCallsIntoTheStartMethodOfActualTracker() throws Exception {

        ReplicatorHealthTrackerProxy proxy = new ReplicatorHealthTrackerProxy();

        IReplicatorHealthTracker trackerMock = mock(IReplicatorHealthTracker.class);

        proxy.setTrackerImplementation(trackerMock);

        proxy.start();

        verify(trackerMock, times(1)).start();
    }

    @Test
    public void makeSureStopCallsIntoTheStopMethodOfActualTracker() throws Exception {

        ReplicatorHealthTrackerProxy proxy = new ReplicatorHealthTrackerProxy();

        IReplicatorHealthTracker trackerMock = mock(IReplicatorHealthTracker.class);

        proxy.setTrackerImplementation(trackerMock);

        proxy.start();
        proxy.stop();

        verify(trackerMock, times(1)).stop();
    }

    @Test
    public void makeSureTheProxyActuallySwitchesToUnderlyingTrackerAfterYouSetIt() throws Exception {

        ReplicatorHealthTrackerProxy proxy = new ReplicatorHealthTrackerProxy();

        IReplicatorHealthTracker trackerMock = mock(IReplicatorHealthTracker.class);

        when(trackerMock.getLastHealthAssessment()).
                thenReturn(ReplicatorHealthAssessment.Normal).
                thenReturn(ReplicatorHealthAssessment.Normal).thenReturn(new ReplicatorHealthAssessment(false, "I'm sick"));

        proxy.setTrackerImplementation(trackerMock);

        proxy.start();

        assertTrue(proxy.getLastHealthAssessment().isOk());
        assertTrue(proxy.getLastHealthAssessment().isOk());
        assertFalse(proxy.getLastHealthAssessment().isOk());
    }
}