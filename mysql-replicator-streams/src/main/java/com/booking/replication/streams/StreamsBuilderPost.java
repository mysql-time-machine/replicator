package com.booking.replication.streams;

import java.util.function.Consumer;

public interface StreamsBuilderPost<Input, Output> {
    StreamsBuilderPost<Input, Output> to(Consumer<Output> consumer);
    StreamsBuilderBuild<Input, Output> post(Consumer<Input> consumer);
    Streams<Input, Output> build();
}
