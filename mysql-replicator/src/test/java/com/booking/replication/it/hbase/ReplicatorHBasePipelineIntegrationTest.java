package com.booking.replication.it.hbase;

import com.booking.replication.commons.services.ServicesControl;

import java.io.IOException;

public interface ReplicatorHBasePipelineIntegrationTest {

    String testName();

    void doAction(ServicesControl service);

    Object getExpectedState();

    Object getActualState() throws IOException;

    boolean actualEqualsExpected(Object expected, Object actual);

}
