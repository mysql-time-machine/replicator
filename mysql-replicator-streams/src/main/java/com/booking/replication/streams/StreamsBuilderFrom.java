package com.booking.replication.streams;

import java.util.function.Supplier;

public interface StreamsBuilderFrom<Input, Output> {
    StreamsBuilderFrom<Input, Output> threads(int threads);

    StreamsBuilderFrom<Input, Output> tasks(int tasks);

    StreamsBuilderFilter<Input, Output> fromPull(Supplier<Input> supplier);

    StreamsBuilderFilter<Input, Output> fromPush();
}
