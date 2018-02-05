package com.booking.replication.model;

import java.io.Serializable;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.List;

@SuppressWarnings("unused")
public interface EventData extends Serializable, EventDecorator {
    static <SubEventData extends EventData> SubEventData decorate(Class<SubEventData> type, InvocationHandler handler) throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
        return EventDecorator.decorate(type, handler);
    }

    static List<Class<? extends EventData>> listSubTypes() {
        return Arrays.asList(
                ByteArrayEventData.class,
                DeleteRowsEventData.class,
                FormatDescriptionEventData.class,
                GTIDEventData.class,
                IntVarEventData.class,
                PreviousGTIDSetEventData.class,
                QueryEventData.class,
                RotateEventData.class,
                RowsQueryEventData.class,
                TableMapEventData.class,
                UpdateRowsEventData.class,
                WriteRowsEventData.class,
                XAPrepareEventData.class,
                XIDEventData.class
        );
    }
}
