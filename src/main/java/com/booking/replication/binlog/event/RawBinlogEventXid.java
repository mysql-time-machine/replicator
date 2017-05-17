package com.booking.replication.binlog.event;

import com.github.shyiko.mysql.binlog.event.XidEventData;
import com.google.code.or.binlog.impl.event.XidEvent;

/**
 * Created by bosko on 5/22/17.
 */
public class RawBinlogEventXid extends RawBinlogEvent {
    public RawBinlogEventXid(Object event) throws Exception {
        super(event);
    }

    public long getXid() {
        if (this.USING_DEPRECATED_PARSER) {
            return ((XidEvent) this.binlogEventV4).getXid();
        } else {
            return ((XidEventData) this.binlogConnectorEvent.getData()).getXid();
        }
    }
}
