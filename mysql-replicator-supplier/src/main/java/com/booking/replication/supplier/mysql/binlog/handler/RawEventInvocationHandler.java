package com.booking.replication.supplier.mysql.binlog.handler;

import com.booking.replication.supplier.model.RawEventData;
import com.booking.replication.supplier.model.RawEventHeaderV4;
import com.booking.replication.supplier.model.RawEventType;
import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.Event;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class RawEventInvocationHandler implements InvocationHandler {

    private final Event event;
    private final Map<String, Class<? extends RawEventData>> eventDataSubTypes;

    private final String gtidSet;

    public RawEventInvocationHandler(BinaryLogClient binaryLogClient, Event event) {
        this.gtidSet = binaryLogClient.getGtidSet();
        this.event = event;
        this.eventDataSubTypes = Stream
                .of(RawEventType.values())
                .map(RawEventType::getDefinition)
                .distinct()
                .collect(
                        Collectors.toMap(
                                (value) -> value.getSimpleName().replace("Raw", "").toLowerCase(),
                                (value) -> value
                        )
                );
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {

        if (method.getName().equals("getHeader")) {
            com.github.shyiko.mysql.binlog.event.EventHeader eventHeader = this.event.getHeader();

            if (eventHeader != null) {
                return RawEventHeaderV4.getProxy(
                        RawEventHeaderV4.class,
                        new RawEventHeaderInvocationHandler(eventHeader)
                );
            } else {
                return null;
            }
        } else if (method.getName().equals("getData")) {
            com.github.shyiko.mysql.binlog.event.EventData eventData = this.event.getData();

            if (eventData != null) {
                return RawEventData.getProxy(
                        this.getEventDataSubType(eventData.getClass()),
                        new RawEventDataInvocationHandler(eventData)
                );
            } else {
                return null;
            }
        }
        else if (method.getName().equals("getGTIDSet")) {
            return this.gtidSet;
        }
        else {
            return this.event.getClass().getMethod(method.getName()).invoke(this.event);
        }
    }

    private Class<? extends RawEventData> getEventDataSubType(Class<?> eventDataType) {
        return this.eventDataSubTypes.get(eventDataType.getSimpleName().toLowerCase());
    }
}
