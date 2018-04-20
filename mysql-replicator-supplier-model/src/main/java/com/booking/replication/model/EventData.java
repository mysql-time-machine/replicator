package com.booking.replication.model;

import java.io.Serializable;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;

@SuppressWarnings("unused")
public interface EventData extends Serializable, EventProxyProvider {

    static <SubEventData extends EventData> SubEventData getProxy(
            Class<SubEventData> type,
            InvocationHandler handler)
        throws
            NoSuchMethodException,
            IllegalAccessException,
            InvocationTargetException,
            InstantiationException {
        return EventProxyProvider.getProxy(type, handler);
    }
}
