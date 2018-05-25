package com.booking.replication.augmenter.pipeline;

import com.booking.replication.supplier.model.RawEventImplementation;

public class TableMapException extends Exception {
    public TableMapException(String message) {
        super(message);
    }

    public TableMapException(String message, RawEventImplementation event) {
        this(String.format(
                "%s\nBinlog Position: %s %s",
                message,
                event.getHeader().getBinlogFileName(),
                event.getHeader().getBinlogPosition()
        ));
    }
}