package com.booking.replication.streams;

import java.util.Arrays;
import java.util.Deque;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.*;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public final class StreamsImplementation<Input, Output> implements Streams<Input, Output> {
    private static final Logger LOG = LogManager.getLogger(StreamsImplementation.class);

    private final int threads;
    private final int tasks;
    private final BiFunction<Input, Integer, Integer> partitioner;
    private final BlockingDeque<Input>[] queues;
    private final long queueTimeout;
    private final Function<Integer, Input> dataSupplierFn;
    private final BiConsumer<Integer, Input> requeue;
    private final Predicate<Input> filter;
    private final Function<Input, Output> process;
    private final Function<Output, Boolean> sink;
    private final BiConsumer<Input, Integer> post;
    private final AtomicBoolean running;
    private final AtomicBoolean handling;

    private ExecutorService executor;
    private Consumer<Exception> handler;

    @SuppressWarnings("unchecked")
    StreamsImplementation(
            int threads,
            int tasks,
            BiFunction<Input, Integer, Integer> partitioner,
            Class<? extends BlockingDeque> queueType,
            int queueSize,
            long queueTimeout,
            Function<Integer, Input> fnGetNextItem,
            Predicate<Input> filter,
            Function<Input, Output> process,
            Function<Output, Boolean> sink,
            BiConsumer<Input, Integer> post
    ) {
        this.threads = threads + 1;
        this.tasks = tasks;

        if (partitioner != null) {
            this.partitioner = partitioner;
        } else {
            this.partitioner = (input, maximum) -> ThreadLocalRandom.current().nextInt(maximum);
        }

        if (queueType != null) {
            this.queues = new BlockingDeque[this.tasks];

            for (int index = 0; index < this.queues.length; index++) {
                try {
                    this.queues[index] = queueType.getConstructor(int.class).newInstance(queueSize);
                } catch (ReflectiveOperationException exception) {
                    throw new RuntimeException(exception);
                }
            }
        } else {
            this.queues = null;
        }

        this.queueTimeout = queueTimeout;

        if (this.queues != null) {
            this.dataSupplierFn = (task) -> {
                while (true) {
                    int size = StreamsImplementation.this.queues[task].size();
                    if (size > 9000) {
                        LOG.warn("Queues are getting big. Queue #" + task + " size: " + size);
                    }
                    try {
                        return StreamsImplementation.this.queues[task].takeFirst();
                    } catch (InterruptedException e) {
                        LOG.info("Queue #" + task + " reader interrupted");
                    }
                }
            };
            this.requeue = (task, input) -> {
                try {
                    if (!StreamsImplementation.this.queues[task].offerFirst(input, queueTimeout, TimeUnit.SECONDS)) {
                        this.handleException(new StreamsException(String.format("Max waiting time exceeded while requeue setSink internal buffer: %d seconds", queueTimeout)));
                    }
                } catch (InterruptedException exception) {
                    Thread.currentThread().interrupt();
                    this.handleException(exception);
                }
            };
        } else if (fnGetNextItem != null) {
            this.dataSupplierFn = fnGetNextItem;
            this.requeue = null;
        } else {
            this.dataSupplierFn = null;
            this.requeue = null;
        }

        this.filter = (filter != null) ? (filter) : (input -> true);
        this.process = (process != null) ? (process) : (input -> (Output) input);
        this.sink = (sink != null) ? (sink) : (output -> true);

        this.post = (post != null) ? (post) : ((output, executing) -> {
        });
        this.running = new AtomicBoolean();
        this.handling = new AtomicBoolean();
        this.handler = (exception) -> StreamsImplementation.LOG.error("error inside streams", exception);
    }

    private void process(Input input, int task) {
        if (input != null && this.filter.test(input)) {
            Output output = this.process.apply(input);
            if (output == null) {
            }
            if (output != null && this.sink.apply(output)) {
                this.post.accept(input, task);
            }
        }
    }

    private void handleException(Exception exception) {
        if (!this.handling.getAndSet(true)) {
            new Thread(() -> {
                this.handler.accept(exception);
                this.handling.set(false);
            }).start();
        } else {
            StreamsImplementation.LOG.error("error inside streams", exception);
        }
    }

    @Override
    public final Streams<Input, Output> start() {
        if ((this.queues != null || this.dataSupplierFn != null) && !this.running.getAndSet(true) && this.executor == null) {
            Consumer<Integer> consumer = (partitionNumber) -> {
                Input input = null;

                try {
                    while (this.running.get()) {
                        input = this.dataSupplierFn.apply(partitionNumber);
                        this.process(input, partitionNumber);
                        input = null;
                    }
                } catch (Exception exception) {
                    this.handleException(exception);
                } finally {
                    if (this.requeue != null && input != null) {
                        this.requeue.accept(partitionNumber, input);
                    }
                }
            };

            LOG.info("Starting a stream with #" + this.threads + " FixedThreadPool");
            this.executor = Executors.newFixedThreadPool(this.threads);

            for (int index = 0; index < this.tasks; index++) {
                final int partitionNumber = index;

                this.executor.execute(() -> consumer.accept(partitionNumber));
            }
        }

        return this;
    }

    @Override
    public final Streams<Input, Output> wait(long timeout, TimeUnit unit) throws InterruptedException {
        if (this.running.get() && this.executor != null) {
            this.executor.awaitTermination(timeout, unit);
        }

        return this;
    }

    @Override
    public final void join() throws InterruptedException {
        this.wait(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
    }

    @Override
    public final void stop() throws InterruptedException {
        if (this.running.getAndSet(false) && this.executor != null) {
            try {
                this.executor.shutdown();
                this.executor.awaitTermination(5L, TimeUnit.SECONDS);
            } finally {
                this.executor.shutdownNow();
                this.executor = null;
            }
        }
    }

    @Override
    public final void onException(Consumer<Exception> handler) {
        Objects.requireNonNull(handler);
        this.handler = handler;
    }

    @Override
    public final void push(Input input) {
        if (this.queues == null && this.dataSupplierFn == null) {
            try {
                this.process(input, 0);
            } catch (Exception exception) {
                this.handleException(exception);
            }
        }

        // if queues are available then dataSupplierFn is internally initialized
        // to a lambda that polls the queue. This happens even if dataSupplierFn
        // is specified in pipeline configuration, it will still get overridden
        // by queue poller.
        else if (this.queues != null) {

            Objects.requireNonNull(this.queues, "queues must not be null");

            if (!this.running.get()) {
                throw new IllegalStateException("Streams has stopped.");
            }

            try {
                if (!StreamsImplementation.this.queues[this.partitioner.apply(input, this.tasks)].offer(input, this.queueTimeout, TimeUnit.SECONDS)) {
                    LOG.warn("Push: offer timeout");
                    this.handleException(new StreamsException(String.format("Max waiting time exceeded while writing setSink internal buffer: %d", this.queueTimeout)));
                }
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
                this.handleException(exception);
            }
        }
    }

    @Override
    public final int size() {
        if (this.queues != null) {
            return Arrays.stream(this.queues).mapToInt(Deque::size).sum();
        } else {
            return 0;
        }
    }
}
