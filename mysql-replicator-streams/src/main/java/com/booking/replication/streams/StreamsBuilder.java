package com.booking.replication.streams;

import java.util.Objects;
import java.util.Set;
import java.util.function.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class StreamsBuilder<Input, Output> implements
        StreamsBuilderFrom<Input, Output>,
        StreamsBuilderFilter<Input, Output>,
        StreamsBuilderTo<Input, Output>,
        StreamsBuilderPost<Input, Output>,
        StreamsBuilderBuild<Input, Output> {
    private static final Logger log = Logger.getLogger(StreamsBuilder.class.getName());

    private int threads;
    private int tasks;
    private Supplier<Input> from;
    private Predicate<Input> filter;
    private Function<Input, Output> process;
    private Consumer<Output> to;
    private BiConsumer<Input, Set<Input>> post;

    private StreamsBuilder(Supplier<Input> from, Predicate<Input> filter, Function<Input, Output> process) {
        this.threads = 1;
        this.tasks = 1;
        this.from = from;
        this.filter = filter;
        this.process = process;
        this.to = (value) -> StreamsBuilder.log.log(Level.FINEST, value.toString());
        this.post = (value, current) -> StreamsBuilder.log.log(Level.FINEST, value.toString());
    }

    @SuppressWarnings("unchecked")
    StreamsBuilder() {
        this(
            null,
            (value) -> true,
            (value) -> {
                StreamsBuilder.log.log(Level.FINEST, value.toString());
                return (Output) value;
            }
        );
    }

    @Override
    public final StreamsBuilderFrom<Input, Output> threads(int threads) {
        this.threads = threads;
        return this;
    }

    @Override
    public final StreamsBuilderFrom<Input, Output> tasks(int tasks) {
        this.tasks = tasks;
        return this;
    }

    @Override
    public final StreamsBuilderFilter<Input, Output> fromPull(Supplier<Input> supplier) {
        Objects.requireNonNull(supplier);
        this.from = supplier;
        return this;
    }

    @Override
    public final StreamsBuilderFilter<Input, Output> fromPush() {
        this.from = null;
        return this;
    }

    @Override
    public final StreamsBuilderTo<Input, Output> filter(Predicate<Input> predicate) {
        Objects.requireNonNull(predicate);
        this.filter = predicate;
        return this;
    }

    @Override
    public final <To> StreamsBuilderTo<Input, To> process(Function<Output, To> function) {
        Objects.requireNonNull(function);
        return new StreamsBuilder<>(this.from, this.filter, this.process.andThen(function));
    }

    @Override
    public final StreamsBuilderPost<Input, Output> to(Consumer<Output> consumer) {
        Objects.requireNonNull(consumer);
        this.to = this.to.andThen(consumer);
        return this;
    }

    @Override
    public final StreamsBuilderBuild<Input, Output> post(BiConsumer<Input, Set<Input>> consumer) {
        Objects.requireNonNull(consumer);
        this.post = this.post.andThen(consumer);
        return this;
    }

    @Override
    public final Streams<Input, Output> build() {
        return new StreamsImplementation<>(this.threads, this.tasks, this.from, this.filter, this.process, this.to, this.post);
    }
}
