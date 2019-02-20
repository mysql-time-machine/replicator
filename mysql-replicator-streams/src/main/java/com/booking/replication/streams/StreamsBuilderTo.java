package com.booking.replication.streams;

import java.util.function.Function;

public interface StreamsBuilderTo<Input, Output> {
    <To> StreamsBuilderTo<Input, To> process(Function<Output, To> process);

    StreamsBuilderPost<Input, Output> setSink(Function<Output, Boolean> to);
}
