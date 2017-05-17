package com.booking.replication.binlog;

import com.booking.replication.binlog.event.RawBinlogEvent;

/**
 * Created by edmitriev on 7/24/17.
 */
public class EventPosition {

    public static String getEventBinlogFileName(RawBinlogEvent event) {
        return event.getBinlogFilename();
    }

    public static long getEventBinlogPosition(RawBinlogEvent event) {
        return event.getPosition();
    }

    public static long getEventBinlogNextPosition(RawBinlogEvent event) {
        return event.getNextPosition();
    }

    public static String getEventBinlogFileNameAndPosition(RawBinlogEvent event) {
        return event.getBinlogFilename() + ":" + event.getPosition();
    }
}
