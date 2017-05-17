package com.booking.replication.binlog.event;

/**
 * Created by bosko on 5/22/17.
 */
public class RawBinlogEventFormatDescription extends RawBinlogEvent {
    public RawBinlogEventFormatDescription(Object event) throws Exception {
        super(event);
    }
}
