package com.booking.replication.augmenter.pipeline;

import com.booking.replication.supplier.model.RawEvent;

import com.booking.replication.supplier.model.EventHeader;

public class EventPosition {

    public static String getEventBinlogFileName(RawEvent event) {
        return event.getHeader().getBinlogFileName();
    }

    public static long getEventBinlogPosition(RawEvent event) {
        return event.getHeader().getBinlogPosition();
    }

    public static long getEventBinlogNextPosition(RawEvent event) {
        return event.getHeader().getNextBinlogPosition();
    }

    public static String getEventBinlogFileNameAndPosition(RawEvent event) {
        return event.getHeader().getBinlogFileName()
                + ":"
                + event.getHeader().getBinlogPosition();
    }
}