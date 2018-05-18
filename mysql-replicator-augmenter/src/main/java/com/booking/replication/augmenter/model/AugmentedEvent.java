package com.booking.replication.augmenter.model;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Created by bdevetak on 4/20/18.
 */
public interface AugmentedEvent extends Serializable, AugmentedEventProxyProvider {

    <Header extends AugmentedEventHeader> Header getHeader();

    <Data extends AugmentedEventData> Data getData();

    static AugmentedEvent getAugmentedEventProxy(InvocationHandler handler)
            throws
            NoSuchMethodException,
            IllegalAccessException,
            InvocationTargetException,
            InstantiationException
    {
        return AugmentedEventProxyProvider.getProxy(AugmentedEvent.class, handler);
    }

    String getTableName();
}
