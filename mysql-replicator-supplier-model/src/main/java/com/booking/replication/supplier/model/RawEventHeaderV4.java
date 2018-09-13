package com.booking.replication.supplier.model;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;

@SuppressWarnings("unused")
public interface RawEventHeaderV4 extends RawEventHeader {
    long getServerId();

    long getEventLength();

    long getHeaderLength();

    long getDataLength();

    long getPosition();

    long getNextPosition();

    int getFlags();

    static RawEventHeaderV4 getProxy(
            Class<RawEventHeaderV4> eventHeaderV4Class,
            InvocationHandler handler)
        throws
            NoSuchMethodException,
            IllegalAccessException,
            InvocationTargetException,
            InstantiationException {
        return RawEventHeader.getProxy(RawEventHeaderV4.class, handler);
    }
}
