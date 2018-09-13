package com.booking.replication.supplier.model;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;

@SuppressWarnings("unused")
public interface RawEventProxyProvider {
     static <SubClass extends RawEventProxyProvider> SubClass getProxy(
            Class<SubClass> type,
            InvocationHandler handler
        ) throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
        return Proxy
                .getProxyClass(type.getClassLoader(), type)
                .asSubclass(type).getConstructor(InvocationHandler.class)
                .newInstance(handler);
    }
}
