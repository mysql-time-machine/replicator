package com.booking.replication.binlog.event;

/**
 * Created by bosko on 7/24/17.
 */
public class RawBinlogEventStop extends RawBinlogEvent {

    public RawBinlogEventStop(Object event) throws Exception {
        super(event);
    }
}
