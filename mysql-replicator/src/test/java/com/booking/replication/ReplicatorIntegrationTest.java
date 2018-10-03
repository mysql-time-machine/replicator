package com.booking.replication;

import com.booking.replication.commons.services.ServicesControl;

import java.io.IOException;

public interface ReplicatorIntegrationTest {

    String testName();

    void doAction(ServicesControl service);

    Object getExpectedState();

    Object getActualState() throws IOException;

    boolean actualEqualsExpected(Object expected, Object actual);

}
