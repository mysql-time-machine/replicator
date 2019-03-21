package com.booking.replication.supplier.model;

import java.io.Serializable;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.atomic.AtomicReference;

/**
 * RawEvent extends RawEventProxyProvider so it has a method to get the
 * proxy which contains the implementation
 */
@SuppressWarnings("unused")
public interface RawEvent extends Serializable, RawEventProxyProvider {

    public AtomicReference<String> gtidSet = new AtomicReference<>();

    <Header extends RawEventHeader> Header getHeader();

    <Data extends RawEventData> Data getData();

    String getGTIDSet();

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
