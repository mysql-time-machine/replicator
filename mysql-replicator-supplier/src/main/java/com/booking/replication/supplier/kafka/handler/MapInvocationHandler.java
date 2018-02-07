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
    public Object invoke(Object proxy, Method method, Object[] args) {
        if (method.getName().startsWith("get") && method.getName().length() > 3) {
            return this.invokeGet(method, 3);
        } else if (method.getName().startsWith("is") && method.getName().length() > 2) {
            return this.invokeGet(method, 2);
        } else if (method.getName().startsWith("set") && method.getName().length() > 3) {
            return this.invokeSet(method, args[0]);
        } else {
            return method.getReturnType().cast(data.get(method.getName()));
        }
    }

    private Object invokeGet(Method method, int length) {
        return method.getReturnType().cast(data.get(this.getPropertyName(method, length)));
    }

    private Object invokeSet(Method method, Object value) {
        data.put(this.getPropertyName(method, 3), value);
        return null;
    }

    private String getPropertyName(Method method, int length) {
        return String.format(
                "%s%s",
                method.getName().substring(length, length + 1).toLowerCase(),
                method.getName().substring(length + 1)
        );
    }
}
