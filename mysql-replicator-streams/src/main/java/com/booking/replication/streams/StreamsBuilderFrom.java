package com.booking.replication.streams;

import java.util.Deque;
import java.util.function.Supplier;

public interface StreamsBuilderFrom<Input, Output> {
    StreamsBuilderFrom<Input, Output> threads(int threads);

    StreamsBuilderFrom<Input, Output> tasks(int tasks);

    StreamsBuilderFrom<Input, Output> queue();

    StreamsBuilderFrom<Input, Output> queue(Deque<Input> queue);

    StreamsBuilderFilter<Input, Output> fromPull(Supplier<Input> supplier);

    StreamsBuilderFilter<Input, Output> fromPush();
}
