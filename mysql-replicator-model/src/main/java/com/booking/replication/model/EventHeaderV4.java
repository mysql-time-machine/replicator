package com.booking.replication.model;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;

@SuppressWarnings("unused")
public interface EventHeaderV4 extends EventHeader {
    long getServerId();

    long getEventLength();

    default long getHeaderLength() {
        return 19;
    }

    default long getDataLength() {
        return this.getEventLength() - this.getHeaderLength();
    }

    long getNextPosition();

    int getFlags();

    static EventHeaderV4 decorate(InvocationHandler handler) throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
        return EventHeader.decorate(EventHeaderV4.class, handler);
    }
}
