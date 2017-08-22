package com.booking.replication.pipeline.event.handler;

/**
 * Created by edmitriev on 7/14/17.
 */
public class EventHandlerApplyException extends Exception {
    public EventHandlerApplyException(String string) {
        super(string);
    }
    public EventHandlerApplyException(String string, Throwable throwable) {
        super(string, throwable);
    }
}
