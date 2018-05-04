package com.booking.replication.streams;

import java.util.Deque;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class StreamsBuilder<Input, Output> implements
        StreamsBuilderFrom<Input, Output>,
        StreamsBuilderFilter<Input, Output>,
        StreamsBuilderTo<Input, Output>,
        StreamsBuilderPost<Input, Output>,
        StreamsBuilderBuild<Input, Output> {
    private static final Logger LOG = Logger.getLogger(StreamsBuilder.class.getName());

    private int threads;
    private int tasks;
    private BiFunction<Input, Integer, Integer> partitioner;
    private Class<? extends Deque> queueType;
    private Function<Integer, Input> from;
    private Predicate<Input> filter;
    private Function<Input, Output> process;
    private Consumer<Output> to;
    private BiConsumer<Input, Map<Input, AtomicReference<Output>>> post;

    private StreamsBuilder(Function<Integer, Input> from, Predicate<Input> filter, Function<Input, Output> process) {
        this.threads = 1;
        this.tasks = 1;
        this.partitioner = null;
        this.queueType = null;
        this.from = from;
        this.filter = filter;
        this.process = process;
        this.to = (value) -> StreamsBuilder.LOG.log(Level.FINEST, value.toString());
        this.post = (value, executing) -> StreamsBuilder.LOG.log(Level.FINEST, value.toString());
    }

    @SuppressWarnings("unchecked")
    StreamsBuilder() {
        this(
                null,
                (value) -> true,
                (value) -> {
                    StreamsBuilder.LOG.log(Level.FINEST, value.toString());
                    return (Output) value;
                }
        );
    }

    @Override
    public final StreamsBuilderFrom<Input, Output> threads(int threads) {
        if (tasks > 0) {
            this.threads = threads;
            return this;
        } else {
            throw new IllegalArgumentException();
        }
    }

    @Override
    public final StreamsBuilderFrom<Input, Output> tasks(int tasks) {
        if (tasks > 0) {
            this.tasks = tasks;
            return this;
        } else {
            throw new IllegalArgumentException();
        }
    }

    @Override
    public StreamsBuilderFrom<Input, Output> partitioner(BiFunction<Input, Integer, Integer> partitioner) {
        Objects.requireNonNull(partitioner);
        this.partitioner = partitioner;
        return this;
    }

    @Override
    public StreamsBuilderFrom<Input, Output> queue() {
        return this.queue(ConcurrentLinkedDeque.class);
    }

    @Override
    public StreamsBuilderFrom<Input, Output> queue(Class<? extends Deque> queueType) {
        Objects.requireNonNull(queueType);
        this.queueType = queueType;
        return this;
    }

    @Override
    public final StreamsBuilderFilter<Input, Output> fromPull(Function<Integer, Input> supplier) {
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
        this.filter = this.filter.and(predicate);
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
    public final StreamsBuilderBuild<Input, Output> post(Consumer<Input> consumer) {
        Objects.requireNonNull(consumer);
        return this.post((value, executing) -> consumer.accept(value));
    }

    @Override
    public final StreamsBuilderBuild<Input, Output> post(
            BiConsumer<Input, Map<Input, AtomicReference<Output>>> consumer
    ) {
        Objects.requireNonNull(consumer);
        this.post = this.post.andThen(consumer);
        return this;
    }

    @Override
    public final Streams<Input, Output> build() {
        return new StreamsImplementation<>(
                this.threads,
                this.tasks,
                this.partitioner,
                this.queueType,
                this.from,
                this.filter,
                this.process,
                this.to,
                this.post
        );
    }
}
