package com.booking.replication.schema.exception;

import com.booking.replication.binlog.event.RawBinlogEvent;

/**
 * Created by bosko on 11/10/15.
 */
public class TableMapException extends Exception {
    public TableMapException(String message) {
        super(message);
    }

    public TableMapException(String message, RawBinlogEvent event) {
        this(String.format(
                "%s\nBinlog Position: %s",
                message,
                event.getPosition()
                ));
    }


}
