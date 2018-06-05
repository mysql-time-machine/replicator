package com.booking.replication.binlog.event.impl;

import com.booking.replication.binlog.event.impl.BinlogEvent;

/**
 * Created by bosko on 5/22/17.
 */
public class BinlogEventRows extends BinlogEvent {
    public BinlogEventRows(Object event) throws Exception {
        super(event);
    }
}
