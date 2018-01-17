package com.booking.replication.streams;

import java.util.function.Consumer;
import java.util.function.Function;

public interface StreamsBuilderTo<Input, Output> {
    <To> StreamsBuilderTo<Input, To> process(Function<Output, To> processor);
    StreamsBuilderPost<Input, Output> to(Consumer<Output> consumer);
}
