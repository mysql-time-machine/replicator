package com.booking.replication.commons.services;

import java.io.Closeable;

public interface ServicesControl extends Closeable {
    @Override
    void close();

    default String getHost() {
        return "localhost";
    }

    int getPort();

    default String getURL() {
        return String.format("%s:%d", this.getHost(), this.getPort());
    }
}
