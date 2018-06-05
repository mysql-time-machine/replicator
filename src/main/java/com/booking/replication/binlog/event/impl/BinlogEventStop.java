package com.booking.replication.binlog.event.impl;

import com.booking.replication.binlog.event.impl.BinlogEvent;

/**
 * Created by bosko on 7/24/17.
 */
public class BinlogEventStop extends BinlogEvent {

    public BinlogEventStop(Object event) throws Exception {
        super(event);
    }
}
