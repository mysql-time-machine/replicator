package com.booking.replication.model;

import com.booking.replication.model.handler.JSONInvocationHandler;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;

/**
 * Event extends EventProxyProvider so it has a method to get the
 * proxy which contains the implementation
 */
@SuppressWarnings("unused")
public interface Event extends Serializable, EventProxyProvider {
    <Header extends EventHeader> Header getHeader();

    <Data extends EventData> Data getData();

    static Event getProxy(InvocationHandler handler) throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
        return EventProxyProvider.getProxy(Event.class, handler);
    }

    // TODO: remove due to split to RawEvent and AugmentedEvent
    static Event build(ObjectMapper mapper, EventHeader eventHeader, byte[] data) throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException, IOException {
        return new EventImplementation<>(eventHeader, EventData.getProxy(eventHeader.getEventType().getDefinition(), new JSONInvocationHandler(mapper, data)));
    }

    static Event build(ObjectMapper mapper, byte[] header, byte[] data) throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException, IOException {
        return Event.build(mapper, EventHeader.getProxy(new JSONInvocationHandler(mapper, header)), data);
    }
}
