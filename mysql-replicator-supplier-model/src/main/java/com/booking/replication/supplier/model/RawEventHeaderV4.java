package com.booking.replication.supplier.model;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;

@SuppressWarnings("unused")
public interface RawEventHeaderV4 extends RawEventHeader {
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
