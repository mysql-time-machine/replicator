package com.booking.replication.supplier.model;

import java.io.Serializable;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;

/**
 * RawEvent extends RawEventProxyProvider so it has a method to get the
 * proxy which contains the implementation
 */
@SuppressWarnings("unused")
public interface RawEvent extends Serializable, RawEventProxyProvider {
    <Header extends RawEventHeader> Header getHeader();

    <Data extends RawEventData> Data getData();

    static RawEvent getRawEventProxy(InvocationHandler handler)
        throws
            NoSuchMethodException,
            IllegalAccessException,
            InvocationTargetException,
            InstantiationException
    {
        return RawEventProxyProvider.getProxy(RawEvent.class, handler);
    }
}
