package com.booking.replication.it.hbase;

import com.booking.replication.commons.services.ServicesControl;

import java.io.IOException;
import java.util.Map;

public interface ReplicatorHBasePipeline {

    String testName();

    default Map<String, Object> perTestConfiguration(Map<String,Object> genericConfig) {
        return genericConfig;
    }

    void doAction(ServicesControl service);

    Object getExpectedState();

    Object getActualState() throws IOException;

    boolean actualEqualsExpected(Object expected, Object actual);

}
