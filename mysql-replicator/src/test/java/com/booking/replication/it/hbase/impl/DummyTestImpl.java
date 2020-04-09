package com.booking.replication.it.hbase.impl;

import com.booking.replication.commons.services.ServicesControl;
import com.booking.replication.it.hbase.ReplicatorHBasePipelineIntegrationTest;

import java.io.IOException;

public class DummyTestImpl implements ReplicatorHBasePipelineIntegrationTest {
    @Override
    public String testName() {
        return null;
    }

    @Override
    public void doAction(ServicesControl service) {

    }

    @Override
    public Object getExpectedState() {
        return null;
    }

    @Override
    public Object getActualState() throws IOException {
        return null;
    }

    @Override
    public boolean actualEqualsExpected(Object expected, Object actual) {
        return false;
    }
}
