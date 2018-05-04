package com.booking.replication.augmenter.model;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;

/**
 * Created by bdevetak on 4/20/18.
 */
public interface AugmentedEventProxyProvider {

    static <SubClass extends AugmentedEventProxyProvider> SubClass getProxy(
            Class<SubClass>   type,
            InvocationHandler handler
    ) throws
            NoSuchMethodException,
            IllegalAccessException,
            InvocationTargetException,
            InstantiationException {

        return Proxy
                .getProxyClass(type.getClassLoader(), type)
                .asSubclass(type).getConstructor(InvocationHandler.class)
                .newInstance(handler);
    }
}
