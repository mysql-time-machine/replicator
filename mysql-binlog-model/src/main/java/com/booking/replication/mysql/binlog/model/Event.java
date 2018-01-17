package com.booking.replication.mysql.binlog.model;

import java.io.Serializable;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;

@SuppressWarnings("unused")
public interface Event extends Serializable, EventDecorator {
    <Header extends EventHeader> Header getHeader();
    <Data extends EventData> Data getData();

    static Event decorate(InvocationHandler handler) throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
        return EventDecorator.decorate(Event.class, handler);
    }
}
