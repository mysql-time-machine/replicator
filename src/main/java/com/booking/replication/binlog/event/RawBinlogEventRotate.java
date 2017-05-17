package com.booking.replication.binlog.event;

import com.github.shyiko.mysql.binlog.event.RotateEventData;
import com.google.code.or.binlog.impl.event.RotateEvent;

/**
 * Created by bosko on 5/22/17.
 */
public class RawBinlogEventRotate extends RawBinlogEvent {
    public RawBinlogEventRotate(Object event) throws Exception {
        super(event);
    }

    public String getBinlogFileName() {
        if (USING_DEPRECATED_PARSER) {
            return ((RotateEvent) this.getBinlogEventV4()).getBinlogFilename();
        }
        else {
            return ((RotateEventData) this.getBinlogConnectorEvent().getData()).getBinlogFilename();
        }
    }
}
