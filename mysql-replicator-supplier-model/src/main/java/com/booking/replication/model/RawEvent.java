package com.booking.replication.model;

import com.booking.replication.model.handler.JSONInvocationHandler;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;

/**
 * RawEvent extends EventProxyProvider so it has a method to get the
 * proxy which contains the implementation
 */
@SuppressWarnings("unused")
public interface RawEvent extends Serializable, EventProxyProvider {
    <Header extends EventHeader> Header getHeader();

    <Data extends EventData> Data getData();

    static RawEvent getProxy(InvocationHandler handler) throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
        return EventProxyProvider.getProxy(RawEvent.class, handler);
    }

    // TODO: remove due to split to RawEvent and AugmentedEvent
    static RawEvent build(ObjectMapper mapper, EventHeader eventHeader, byte[] data) throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException, IOException {
        return new RawEventImplementation<>(eventHeader, EventData.getProxy(eventHeader.getEventType().getDefinition(), new JSONInvocationHandler(mapper, data)));
    }

    static RawEvent build(ObjectMapper mapper, byte[] header, byte[] data) throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException, IOException {
        return RawEvent.build(mapper, EventHeader.getProxy(new JSONInvocationHandler(mapper, header)), data);
    }
}
