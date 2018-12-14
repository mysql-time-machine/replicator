package com.booking.replication.streams;

import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

@SuppressWarnings("unused")
public interface Streams<Input, Output> {
    Streams<Input, Output> start() throws InterruptedException;

    Streams<Input, Output> wait(long timeout, TimeUnit unit) throws InterruptedException;

    void join() throws InterruptedException;

    void stop() throws InterruptedException;

    void onException(Consumer<Exception> handler);

    void push(Input input);

    int size();

    static <Input> StreamsBuilderFrom<Input, Input> builder() {
        return new StreamsBuilder<>();
    }
}
