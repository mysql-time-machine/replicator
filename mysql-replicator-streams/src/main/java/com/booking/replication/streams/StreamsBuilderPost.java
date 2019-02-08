package com.booking.replication.streams;

import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

public interface StreamsBuilderPost<Input, Output> {
    StreamsBuilderPost<Input, Output> setSink(Function<Output, Boolean> to);

    StreamsBuilderBuild<Input, Output> post(Consumer<Input> post);

    StreamsBuilderBuild<Input, Output> post(BiConsumer<Input, Integer> post);

    Streams<Input, Output> build();
}
