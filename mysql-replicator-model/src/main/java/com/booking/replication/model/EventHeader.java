package com.booking.replication.model;

import java.io.Serializable;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;

@SuppressWarnings("unused")
public interface EventHeader extends Serializable, EventDecorator {
    long getTimestamp();

    EventType getEventType();

    static <SubEventHeader extends EventHeader> SubEventHeader decorate(Class<SubEventHeader> type, InvocationHandler handler) throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
        return EventDecorator.decorate(type, handler);
    }

    static EventHeader decorate(InvocationHandler handler) throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
        return EventHeader.decorate(EventHeader.class, handler);
    }
}
