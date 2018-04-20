package com.booking.replication.augmenter.model;

import java.io.Serializable;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;

/**
 * Created by bdevetak on 4/20/18.
 */

public interface AugmentedEventHeader extends Serializable, AugmentedEventProxyProvider {
    long getTimestamp();

    AugmentedEventType getEventType();

    static <SubAugmentedEventHeader extends AugmentedEventHeader> SubAugmentedEventHeader getProxy(
            Class<SubAugmentedEventHeader> type,
            InvocationHandler handler)
        throws
            NoSuchMethodException,
            IllegalAccessException,
            InvocationTargetException,
            InstantiationException {
        return AugmentedEventProxyProvider.getProxy(type, handler);
    }

    static AugmentedEventHeader getProxy(InvocationHandler handler)
        throws
            NoSuchMethodException,
            IllegalAccessException,
            InvocationTargetException,
            InstantiationException {
        return AugmentedEventHeader.getProxy(AugmentedEventHeader.class, handler);
    }
}

