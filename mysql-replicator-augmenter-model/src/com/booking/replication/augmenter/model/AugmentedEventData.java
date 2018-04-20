package com.booking.replication.augmenter.model;

import java.io.Serializable;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;

/**
 * Created by bdevetak on 4/20/18.
 */

public interface AugmentedEventData extends Serializable, AugmentedEventProxyProvider {
    static <SubEventData extends AugmentedEventData> SubEventData getProxy(
            Class<SubEventData> type,
            InvocationHandler handler)
        throws
            NoSuchMethodException,
            IllegalAccessException,
            InvocationTargetException,
            InstantiationException {
        return AugmentedEventProxyProvider.getProxy(type, handler);
    }
}