package com.booking.replication.streams;

import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

public interface StreamsBuilderPost<Input, Output> {
    StreamsBuilderPost<Input, Output> to(Consumer<Output> consumer);
    StreamsBuilderBuild<Input, Output> post(BiConsumer<Input, Map<Input, AtomicReference<Output>>> consumer);
    Streams<Input, Output> build();
}
