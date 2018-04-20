package com.booking.replication.augmenter.exception;

import com.booking.replication.model.RawEvent;

/**
 * Created by bosko on 11/10/15.
 */
public class TableMapException extends Exception {
    public TableMapException(String message) {
        super(message);
    }

    public TableMapException(String message, RawEvent rawEvent) {
        this(String.format(
                "%s\nBinlog Position: %s",
                message,
                "TODO pos in binlog"
        ));
    }


}
