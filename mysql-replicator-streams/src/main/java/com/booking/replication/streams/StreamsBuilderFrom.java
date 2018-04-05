package com.booking.replication.streams;

import java.util.Deque;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

public interface StreamsBuilderFrom<Input, Output> {
    StreamsBuilderFrom<Input, Output> threads(int threads);

    StreamsBuilderFrom<Input, Output> tasks(int tasks);

    StreamsBuilderFrom<Input, Output> partitioner(BiFunction<Input, Integer, Integer> partitioner);

    StreamsBuilderFrom<Input, Output> queue();

    StreamsBuilderFrom<Input, Output> queue(Class<? extends Deque> queueType);

    StreamsBuilderFilter<Input, Output> fromPull(Function<Integer, Input> supplier);

    StreamsBuilderFilter<Input, Output> fromPush();
}
