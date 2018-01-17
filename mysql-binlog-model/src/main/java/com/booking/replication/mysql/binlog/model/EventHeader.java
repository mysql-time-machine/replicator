package com.booking.replication.mysql.binlog.model;

import java.io.Serializable;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;

@SuppressWarnings("unused")
public interface EventHeader extends Serializable, EventDecorator {
    long getTimestamp();
    EventType getEventType();
    long getServerId();
    long getEventLength();

    default long getHeaderLength() {
        return 19;
    }

    default long getDataLength() {
        return this.getEventLength() - this.getHeaderLength();
    }

    static <SubEventHeader extends EventHeader> SubEventHeader decorate(Class<SubEventHeader> type, InvocationHandler handler) throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
        return EventDecorator.decorate(type, handler);
    }
}
