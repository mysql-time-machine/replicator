package com.booking.replication.commons.services.containers;

import org.testcontainers.containers.Container;

public interface TestContainer<SELF extends Container<SELF>> extends Container<SELF> {

    default String getURL() {
        return String.format("%s:%d", getHost(), getPort());
    }

    default String getHost() {
        return "0.0.0.0";
    }

    int getPort();

    void doStart();
}
