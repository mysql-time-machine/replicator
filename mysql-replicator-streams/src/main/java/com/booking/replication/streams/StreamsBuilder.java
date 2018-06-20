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
    private BiConsumer<Input, Streams.Task> post;

    private StreamsBuilder(
            int threads,
            int tasks,
            BiFunction<Input, Integer, Integer> partitioner,
            Class<? extends Deque> queueType,
            Function<Integer, Input> from,
            Predicate<Input> filter,
            Function<Input, Output> process,
            Consumer<Output> to,
            BiConsumer<Input, Streams.Task> post) {
        this.threads = threads;
        this.tasks = tasks;
        this.partitioner = partitioner;
        this.queueType = queueType;
        this.from = from;
        this.filter = filter;
        this.process = process;
        this.to = to;
        this.post = post;
    }

    StreamsBuilder() {
        this(0, 1, null, null, null, null, null, null, null);
    }

    @Override
    public final StreamsBuilderFrom<Input, Output> threads(int threads) {
        if (threads > 0) {
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
    public final StreamsBuilderFilter<Input, Output> filter(Predicate<Input> predicate) {
        Objects.requireNonNull(predicate);
        return new StreamsBuilder<>(
                this.threads,
                this.tasks,
                this.partitioner,
                this.queueType,
                this.from,
                input -> (this.filter == null || this.filter.test(input)) && predicate.test(input),
                null,
                null,
                null
        );
    }

    @Override
    @SuppressWarnings("unchecked")
    public final <To> StreamsBuilderTo<Input, To> process(Function<Output, To> function) {
        Objects.requireNonNull(function);
        return new StreamsBuilder<>(
                this.threads,
                this.tasks,
                this.partitioner,
                this.queueType,
                this.from,
                this.filter,
                input -> {
                    Output output = (this.process != null)?(this.process.apply(input)):((Output) input);
                    if (output != null) {
                        return function.apply(output);
                    } else {
                        return null;
                    }
                },
                null,
                null
        );
    }

    @Override
    public final StreamsBuilderPost<Input, Output> to(Consumer<Output> consumer) {
        Objects.requireNonNull(consumer);
        return new StreamsBuilder<>(
                this.threads,
                this.tasks,
                this.partitioner,
                this.queueType,
                this.from,
                this.filter,
                this.process,
                output -> {
                    if (this.to != null) {
                        this.to.accept(output);
                    }

                    if (output != null) {
                        consumer.accept(output);
                    }
                },
                null
        );
    }

    @Override
    public final StreamsBuilderBuild<Input, Output> post(Consumer<Input> consumer) {
        Objects.requireNonNull(consumer);
        return this.post((input, executing) -> consumer.accept(input));
    }

    @Override
    public final StreamsBuilderBuild<Input, Output> post(BiConsumer<Input, Streams.Task> consumer) {
        Objects.requireNonNull(consumer);
        return new StreamsBuilder<>(
                this.threads,
                this.tasks,
                this.partitioner,
                this.queueType,
                this.from,
                this.filter,
                this.process,
                this.to,
                (input, tasks) -> {
                    if (this.post != null) {
                        this.post.accept(input, tasks);
                    }

                    if (input != null) {
                        consumer.accept(input, tasks);
                    }
                }
        );
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
