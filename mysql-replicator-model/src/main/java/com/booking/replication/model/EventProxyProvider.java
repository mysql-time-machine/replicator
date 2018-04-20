package com.booking.replication.model;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;

@SuppressWarnings("unused")
public interface EventProxyProvider {
    static <SubClass extends EventProxyProvider> SubClass getProxy(
            Class<SubClass> type,
            InvocationHandler handler
        ) throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {

        return Proxy
                .getProxyClass(type.getClassLoader(), type)
                .asSubclass(type).getConstructor(InvocationHandler.class)
                .newInstance(handler);
    }
}
