package com.booking.replication.streams;

import java.util.Deque;
import java.util.function.BiFunction;
import java.util.function.Function;

public interface StreamsBuilderFrom<Input, Output> {

    StreamsBuilderFrom<Input, Output> threads(int threads);

    StreamsBuilderFrom<Input, Output> tasks(int tasks);

    StreamsBuilderFrom<Input, Output> partitioner(BiFunction<Input, Integer, Integer> partitioner);

    StreamsBuilderFrom<Input, Output> useDefaultQueueType();

    StreamsBuilderFrom<Input, Output> setQueueType(Class<? extends Deque> queueType);

    StreamsBuilderFilter<Input, Output> setDataSupplier(Function<Integer, Input> supplier);

    StreamsBuilderFilter<Input, Output> usePushMode();

}
