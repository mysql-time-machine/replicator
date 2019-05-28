package com.booking.replication.streams;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Objects;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.function.*;

public final class StreamsBuilder<Input, Output> implements
        StreamsBuilderFrom<Input, Output>,
        StreamsBuilderFilter<Input, Output>,
        StreamsBuilderTo<Input, Output>,
        StreamsBuilderPost<Input, Output>,
        StreamsBuilderBuild<Input, Output> {
    private static final Logger LOG = LogManager.getLogger(StreamsBuilder.class);

    private int threads;
    private int tasks;
    private BiFunction<Input, Integer, Integer> partitioner;
    private Class<? extends BlockingDeque> queueType;
    private int queueSize;
    private long queueTimeout;
    private Function<Integer, Input> dataSupplierFn;
    private Predicate<Input> filter;
    private Function<Input, Output> process;
    private Function<Output, Boolean> to;
    private BiConsumer<Input, Integer> post;

    private StreamsBuilder(
            int threads,
            int tasks,
            BiFunction<Input, Integer, Integer> partitioner,
            Class<? extends BlockingDeque> queueType,
            int queueSize,
            long queueTimeout,
            Function<Integer, Input> dataSupplierFn,
            Predicate<Input> filter,
            Function<Input, Output> process,
            Function<Output, Boolean> to,
            BiConsumer<Input, Integer> post) {
        this.threads = threads;
        this.tasks = tasks;
        this.partitioner = partitioner;
        this.queueType = queueType;
        this.queueSize = queueSize;
        this.queueTimeout = queueTimeout;
        this.dataSupplierFn = dataSupplierFn;
        this.filter = filter;
        this.process = process;
        this.to = to;
        this.post = post;
    }

    StreamsBuilder() {
        this(0, 1, null, null, Integer.MAX_VALUE, Long.MAX_VALUE, null, null, null, null, null);
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
    public final StreamsBuilderFrom<Input, Output> queueSize(int queueSize) {
        if (queueSize > 0) {
            this.queueSize = queueSize;
            return this;
        } else {
            throw new IllegalArgumentException();
        }
    }

    @Override
    public final StreamsBuilderFrom<Input, Output> queueTimeout(long queueTimeout, TimeUnit timeUnit) {
        if (queueTimeout > 0) {
            this.queueTimeout = timeUnit.toSeconds(queueTimeout);
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
    public StreamsBuilderFrom<Input, Output> useDefaultQueueType() {
        return this.setQueueType(LinkedBlockingDeque.class);
    }

    @Override
    public StreamsBuilderFrom<Input, Output> setQueueType(Class<? extends BlockingDeque> queueType) {
        Objects.requireNonNull(queueType);
        this.queueType = queueType;
        return this;
    }

    @Override
    public final StreamsBuilderFilter<Input, Output> setDataSupplier(Function<Integer, Input> supplier) {
        Objects.requireNonNull(supplier);
        this.dataSupplierFn = supplier;
        return this;
    }

    @Override
    public final StreamsBuilderFilter<Input, Output> usePushMode() {
        // here we just make sure that there is no custom consumer to  get the
        // data for the pipeline. Under the hood, this can have two scenarios:
        //      1. if queues exists, the consumer will be created as a simple queue poller, but pipeline will
        //         externally still require call to push()
        //      2. if queues do not exists, then calls to push() will short circuit the data to process().
        this.dataSupplierFn = null;
        return this;
    }

    @Override
    public final StreamsBuilderFilter<Input, Output> filter(Predicate<Input> filter) {
        Objects.requireNonNull(filter);
        return new StreamsBuilder<>(
                this.threads,
                this.tasks,
                this.partitioner,
                this.queueType,
                this.queueSize,
                this.queueTimeout,
                this.dataSupplierFn,
                input -> (this.filter == null || this.filter.test(input)) && filter.test(input),
                null,
                null,
                null
        );
    }

    @Override
    @SuppressWarnings("unchecked")
    public final <To> StreamsBuilderTo<Input, To> process(Function<Output, To> process) {
        Objects.requireNonNull(process);
        return new StreamsBuilder<>(
                this.threads,
                this.tasks,
                this.partitioner,
                this.queueType,
                this.queueSize,
                this.queueTimeout,
                this.dataSupplierFn,
                this.filter,
                input -> {
                    Output output = (this.process != null)?(this.process.apply(input)):((Output) input);
                    if (output != null) {
                        return process.apply(output);
                    } else {
                        return null;
                    }
                },
                null,
                null
        );
    }

    @Override
    public final StreamsBuilderPost<Input, Output> setSink(Function<Output, Boolean> to) {
        Objects.requireNonNull(to);
        return new StreamsBuilder<>(
                this.threads,
                this.tasks,
                this.partitioner,
                this.queueType,
                this.queueSize,
                this.queueTimeout,
                this.dataSupplierFn,
                this.filter,
                this.process,
                output -> {
                    boolean result = true;

                    if (this.to != null) {
                        result = this.to.apply(output);
                    }

                    if (output != null && result) {
                        result = to.apply(output);
                    }

                    return result;
                },
                null
        );
    }

    @Override
    public final StreamsBuilderBuild<Input, Output> post(Consumer<Input> post) {
        Objects.requireNonNull(post);
        return this.post((input, task) -> post.accept(input));
    }

    @Override
    public final StreamsBuilderBuild<Input, Output> post(BiConsumer<Input, Integer> post) {
        Objects.requireNonNull(post);
        return new StreamsBuilder<>(
                this.threads,
                this.tasks,
                this.partitioner,
                this.queueType,
                this.queueSize,
                this.queueTimeout,
                this.dataSupplierFn,
                this.filter,
                this.process,
                this.to,
                (input, task) -> {
                    if (this.post != null) {
                        this.post.accept(input, task);
                    }

                    if (input != null) {
                        post.accept(input, task);
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
                this.queueSize,
                this.queueTimeout,
                this.dataSupplierFn,
                this.filter,
                this.process,
                this.to,
                this.post
        );
    }
}
