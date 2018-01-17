package com.booking.replication.streams;

import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

public interface StreamsBuilderFilter<Input, Output> {
    StreamsBuilderTo<Input, Output> filter(Predicate<Input> filter);
    <To> StreamsBuilderTo<Input, To> process(Function<Output, To> processor);
    StreamsBuilderPost<Input, Output> to(Consumer<Output> consumer);
}
