package com.booking.replication.commons.services;

import org.testcontainers.containers.GenericContainer;

import java.io.Closeable;

public interface ServicesControl extends Closeable {
    @Override
    void close();

    default String getHost() {
        return "0.0.0.0";
    }

    int getPort();

    default String getURL() {
        return String.format("%s:%d", this.getHost(), this.getPort());
    }

    GenericContainer<?> getContainer();
}
