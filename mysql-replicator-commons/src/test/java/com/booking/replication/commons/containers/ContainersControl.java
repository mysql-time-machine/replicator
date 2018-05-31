package com.booking.replication.commons.containers;

import java.io.Closeable;

public interface ContainersControl extends Closeable {
    @Override
    void close();

    String getURL();
}
