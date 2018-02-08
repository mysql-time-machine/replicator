package com.booking.replication.model;

import java.io.Serializable;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.List;

@SuppressWarnings("unused")
public interface EventData extends Serializable, EventDecorator {
    static <SubEventData extends EventData> SubEventData decorate(Class<SubEventData> type, InvocationHandler handler) throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
        return EventDecorator.decorate(type, handler);
    }
}
