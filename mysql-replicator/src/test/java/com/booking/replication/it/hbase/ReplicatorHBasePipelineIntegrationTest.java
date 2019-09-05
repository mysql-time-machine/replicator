package com.booking.replication.it.hbase;

import com.booking.replication.commons.services.containers.TestContainer;

import java.io.IOException;

public interface ReplicatorHBasePipelineIntegrationTest {

    String testName();

    void doAction(TestContainer container);

    Object getExpectedState();

    Object getActualState() throws IOException;

    boolean actualEqualsExpected(Object expected, Object actual);

}
