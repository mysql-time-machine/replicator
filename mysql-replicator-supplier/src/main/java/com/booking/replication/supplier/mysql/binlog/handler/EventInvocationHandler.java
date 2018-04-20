package com.booking.replication.supplier.mysql.binlog.handler;

import com.booking.replication.supplier.model.EventData;
import com.booking.replication.supplier.model.EventHeaderV4;
import com.booking.replication.supplier.model.RawEventType;
import com.github.shyiko.mysql.binlog.event.Event;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class EventInvocationHandler implements InvocationHandler {
    private final Event event;
    private final Map<String, Class<? extends EventData>> eventDataSubTypes;

    public EventInvocationHandler(Event event) {
        this.event = event;
        this.eventDataSubTypes = Stream
                .of(RawEventType.values())
                .map(RawEventType::getDefinition)
                .distinct()
                .collect(
                        Collectors.toMap(
                                (value) -> value.getSimpleName().toLowerCase(),
                                (value) -> value
                        )
                );
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        if (method.getName().equals("getHeader")) {
            com.github.shyiko.mysql.binlog.event.EventHeader eventHeader = this.event.getHeader();

            if (eventHeader != null) {
                return EventHeaderV4.getProxy(
                        new EventHeaderInvocationHandler(eventHeader)
                );
            } else {
                return null;
            }
        } else if (method.getName().equals("getData")) {
            com.github.shyiko.mysql.binlog.event.EventData eventData = this.event.getData();

            if (eventData != null) {
                return EventData.getProxy(
                        this.getEventDataSubType(eventData.getClass()),
                        new EventDataInvocationHandler(eventData)
                );
            } else {
                return null;
            }
        } else {
            return this.event.getClass().getMethod(method.getName()).invoke(this.event);
        }
    }

    private Class<? extends EventData> getEventDataSubType(Class<?> eventDataType) {
        return this.eventDataSubTypes.get(eventDataType.getSimpleName().toLowerCase());
    }
}
