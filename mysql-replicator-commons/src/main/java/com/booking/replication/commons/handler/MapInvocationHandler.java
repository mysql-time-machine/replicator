package com.booking.replication.commons.handler;

import java.beans.Introspector;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.Objects;

public class MapInvocationHandler implements InvocationHandler {
    private final Map<String, Object> map;

    public MapInvocationHandler(Map<String, Object> map) {
        Objects.requireNonNull(map);

        this.map = map;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] arguments) {
        if (arguments.length == 0) {
            if (method.getName().startsWith("get") && method.getName().length() > 3) {
                return this.invokeGet(method, 3);
            } else if (method.getName().startsWith("is") && method.getName().length() > 2) {
                return this.invokeGet(method, 2);
            } else if (method.getName().equals("toString")) {
                return this.map.toString();
            }
        } else if (arguments.length == 1 && method.getName().startsWith("set") && method.getName().length() > 3) {
            return this.invokeSet(method, arguments[0]);
        }

        return method.getReturnType().cast(this.map.get(method.getName()));
    }

    private Object invokeGet(Method method, int length) {
        Object value = this.map.get(this.getPropertyName(method, length));

        if (value == null) {
            return null;
        }

        if (method.getReturnType().isEnum()) {
            return Enum.valueOf(method.getReturnType().asSubclass(Enum.class), value.toString());
        } else if (method.getReturnType().isPrimitive() && !method.getReturnType().equals(value.getClass())) {
            try {
                return method.getReturnType().getConstructor(String.class).newInstance(value.toString());
            } catch (ReflectiveOperationException exception) {
                return value;
            }
        } else {
            return value;
        }
    }

    private Object invokeSet(Method method, Object value) {
        this.map.put(this.getPropertyName(method, 3), value);
        return null;
    }

    private String getPropertyName(Method method, int length) {
        return Introspector.decapitalize(method.getName().substring(length));
    }
}
