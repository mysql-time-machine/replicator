package com.booking.replication.schema.exception;

import com.google.code.or.binlog.BinlogEventV4;

/**
 * Created by bosko on 11/10/15.
 */
public class TableMapException extends Throwable {
    public TableMapException(String message) {
        super(message);
    }

    public TableMapException(String message, BinlogEventV4 event) {
        this(String.format(
                "%s\nBinlog Position: %s",
                message,
                event.getHeader().getPosition()
                ));
    }


}
