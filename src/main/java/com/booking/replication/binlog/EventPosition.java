package com.booking.replication.binlog;

import com.booking.replication.binlog.event.IBinlogEvent;

/**
 * Created by edmitriev on 7/24/17.
 */
public class EventPosition {

    public static String getEventBinlogFileName(IBinlogEvent event) {
        return event.getBinlogFilename();
    }

    public static long getEventBinlogPosition(IBinlogEvent event) {
        return event.getPosition();
    }

    public static long getEventBinlogNextPosition(IBinlogEvent event) {
        return event.getNextPosition();
    }

    public static String getEventBinlogFileNameAndPosition(IBinlogEvent event) {
        return event.getBinlogFilename() + ":" + event.getPosition();
    }
}
