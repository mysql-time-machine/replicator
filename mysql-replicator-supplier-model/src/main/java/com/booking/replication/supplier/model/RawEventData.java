package com.booking.replication.supplier.model;

import java.io.Serializable;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;

@SuppressWarnings("unused")
public interface RawEventData extends Serializable, RawEventProxyProvider {
    static <SubEventData extends RawEventData> SubEventData getProxy(
            Class<SubEventData> type,
            InvocationHandler handler)
        throws
            NoSuchMethodException,
            IllegalAccessException,
            InvocationTargetException,
            InstantiationException {
        return RawEventProxyProvider.getProxy(type, handler);
    }
}
