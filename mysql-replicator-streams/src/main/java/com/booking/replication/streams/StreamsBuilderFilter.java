package com.booking.replication.streams;

import java.util.function.Function;
import java.util.function.Predicate;

public interface StreamsBuilderFilter<Input, Output> {
    StreamsBuilderFilter<Input, Output> filter(Predicate<Input> filter);

    <To> StreamsBuilderTo<Input, To> process(Function<Output, To> process);

    StreamsBuilderPost<Input, Output> setSink(Function<Output, Boolean> to);
}
