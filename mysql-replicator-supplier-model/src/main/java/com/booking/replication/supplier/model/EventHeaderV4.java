package com.booking.replication.supplier.model;

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

    static EventHeaderV4 getProxy(
            Class<EventHeaderV4> eventHeaderV4Class,
            InvocationHandler handler)
        throws
            NoSuchMethodException,
            IllegalAccessException,
            InvocationTargetException,
            InstantiationException {
        return getProxy(EventHeaderV4.class, handler);
    }
}
