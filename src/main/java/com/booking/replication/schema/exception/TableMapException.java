package com.booking.replication.schema.exception;

import com.booking.replication.binlog.event.IBinlogEvent;

/**
 * Created by bosko on 11/10/15.
 */
public class TableMapException extends Exception {
    public TableMapException(String message) {
        super(message);
    }

    public TableMapException(String message, IBinlogEvent event) {
        this(String.format(
                "%s\nBinlog Position: %s",
                message,
                event.getPosition()
                ));
    }


}
