package com.booking.replication.supplier.mysql.binlog.handler;

import com.github.shyiko.mysql.binlog.event.EventData;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class RawEventDataInvocationHandler implements InvocationHandler {

    private static final Logger LOG = LogManager.getLogger(RawEventDataInvocationHandler.class);

    private EventData eventData;
    private Map<String, Method> methodMap;

    public RawEventDataInvocationHandler(EventData eventData) {
        this.eventData = eventData;
        this.methodMap = Stream.of(
                eventData.getClass().getDeclaredMethods()
        ).filter(
                (method) -> method.getName().startsWith("get")
        ).collect(
                Collectors.toMap(
                        (value) -> value.getName().toLowerCase(),
                        (value) -> value
                )
        );
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        return this.methodMap.computeIfAbsent(
                method.getName().toLowerCase(),
                (key) -> {
                    try {
                        return this.eventData.getClass().getMethod(method.getName());
                    } catch (NoSuchMethodException exception) {
                        LOG.error(exception.getMessage(), exception);
                        throw new RuntimeException(exception);
                    }
                }
        ).invoke(this.eventData);
    }
}
