package com.booking.replication.mysql.binlog.supplier.connector.handler;

import com.booking.replication.mysql.binlog.model.EventType;
import com.github.shyiko.mysql.binlog.event.EventHeader;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;

public class EventHeaderInvocationHandler implements InvocationHandler {
    private EventHeader eventHeader;

    public EventHeaderInvocationHandler(EventHeader eventHeader) {
        this.eventHeader = eventHeader;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        if (method.getName().equals("getEventType")) {
            return EventType.valueOf(eventHeader.getEventType().name());
        } else {
            return this.eventHeader.getClass().getMethod(method.getName()).invoke(this.eventHeader);
        }
    }
}
