package com.booking.replication.streams;

import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

public interface StreamsBuilderPost<Input, Output> {
    StreamsBuilderPost<Input, Output> to(Consumer<Output> consumer);
    StreamsBuilderBuild<Input, Output> post(BiConsumer<Input, Set<Input>> consumer);
    Streams<Input, Output> build();
}
