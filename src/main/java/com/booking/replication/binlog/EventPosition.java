package com.booking.replication.binlog;

import com.google.code.or.binlog.BinlogEventV4;
import com.google.code.or.binlog.impl.event.AbstractBinlogEventV4;

/**
 * Created by edmitriev on 7/24/17.
 */
public class EventPosition {

    public static String getEventBinlogFileName(BinlogEventV4 event) {
        if (event instanceof AbstractBinlogEventV4) return ((AbstractBinlogEventV4) event).getBinlogFilename();

        throw new RuntimeException("Can't get binlog filename for unknown event type: " + event);
    }

    public static long getEventBinlogPosition(BinlogEventV4 event) {
        return event.getHeader().getPosition();
    }

    public static long getEventBinlogNextPosition(BinlogEventV4 event) {
        return event.getHeader().getNextPosition();
    }

    public static String getEventBinlogFileNameAndPosition(BinlogEventV4 event) {
        return getEventBinlogFileName(event) + ":" + getEventBinlogPosition(event);
    }
}
