package com.booking.replication.supplier.model;

import java.io.Serializable;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;

@SuppressWarnings("unused")
public interface RawEventHeader extends Serializable, RawEventProxyProvider {
    long getTimestamp();

    RawEventType getEventType();

    static <SubEventHeader extends RawEventHeader> SubEventHeader getProxy(Class<SubEventHeader> type, InvocationHandler handler) throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
        return RawEventProxyProvider.getProxy(type, handler);
    }

    static RawEventHeader getProxy(InvocationHandler handler) throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
        return RawEventHeader.getProxy(RawEventHeader.class, handler);
    }
}
