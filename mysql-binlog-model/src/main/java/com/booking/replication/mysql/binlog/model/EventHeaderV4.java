package com.booking.replication.mysql.binlog.model;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;

public interface EventHeaderV4 extends EventHeader {
    long getNextPosition();
    int getFlags();

    static EventHeaderV4 decorate(InvocationHandler handler) throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
        return EventHeader.decorate(EventHeaderV4.class, handler);
    }
}
