package com.booking.replication.supplier.kafka.handler;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.Map;

public class MapInvocationHandler implements InvocationHandler {
    private final Map<String, Object> data;

    public MapInvocationHandler(Map<String, Object> data) {
        this.data = data;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        String methodName = method.getName().toLowerCase();

        if (methodName.startsWith("get") && methodName.length() > 3) {
            return method.getReturnType().cast(data.get(methodName.substring(3)));
        } else if (methodName.startsWith("is") && methodName.length() > 2) {
            return method.getReturnType().cast(data.get(methodName.substring(2)));
        } else if (methodName.startsWith("set") && methodName.length() > 3) {
            return method.getReturnType().cast(data.put(methodName.substring(3), args[0]));
        } else {
            return method.getReturnType().cast(data.get(methodName));
        }
    }
}
