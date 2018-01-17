package com.booking.replication.streams;

import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public interface Streams<Input, Output> {
    Streams<Input, Output> start() throws InterruptedException;
    Streams<Input, Output> wait(long timeout, TimeUnit unit) throws InterruptedException;
    void join() throws InterruptedException;
    void stop() throws InterruptedException;
    void onException(Consumer<Exception> handler);
    boolean push(Input input);

    static <Input> StreamsBuilderFrom<Input, Input> builder() {
        return new StreamsBuilder<>();
    }
}
