package com.booking.replication.supplier.model;

import com.booking.replication.supplier.model.handler.JSONInvocationHandler;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;

/**
 * RawEvent extends EventProxyProvider so it has a method to get the
 * proxy which contains the implementation
 */
@SuppressWarnings("unused")
public interface RawEvent extends Serializable, EventProxyProvider {

    <Header extends EventHeader> Header getHeader();

    <Data extends EventData> Data getData();

    void overrideTimestamp(long timestamp);

    Long getTimestamp();

    static RawEvent getRawEventProxy(InvocationHandler handler)
        throws
            NoSuchMethodException,
            IllegalAccessException,
            InvocationTargetException,
            InstantiationException
    {
        return EventProxyProvider.getProxy(RawEvent.class, handler);
    }

    void setTimestamp(long timestamp);
}
