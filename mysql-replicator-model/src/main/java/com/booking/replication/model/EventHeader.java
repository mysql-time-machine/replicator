package com.booking.replication.model;

import java.io.Serializable;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;

@SuppressWarnings("unused")
public interface EventHeader extends Serializable, EventProxyProvider {
    long getTimestamp();

    EventType getEventType();

    static <SubEventHeader extends EventHeader> SubEventHeader getProxy(Class<SubEventHeader> type, InvocationHandler handler) throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
        return EventProxyProvider.getProxy(type, handler);
    }

    static EventHeader getProxy(InvocationHandler handler) throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
        return EventHeader.getProxy(EventHeader.class, handler);
    }
}
