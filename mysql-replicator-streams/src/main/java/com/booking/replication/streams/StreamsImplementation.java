package com.booking.replication.streams;

import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class StreamsImplementation<Input, Output> implements Streams<Input, Output> {
    private static final Logger LOG = Logger.getLogger(StreamsImplementation.class.getName());

    private final int tasks;
    private final BiFunction<Input, Integer, Integer> partitioner;
    private final Deque<Input>[] queues;
    private final ExecutorService executor;
    private final Function<Integer, Input> from;
    private final BiConsumer<Integer, Input> requeue;
    private final Predicate<Input> filter;
    private final Function<Input, Output> process;
    private final Consumer<Output> to;
    private final BiConsumer<Input, Map<Input, AtomicReference<Output>>> post;
    private final Map<Input, AtomicReference<Output>> executing;
    private final Map<Input, AtomicReference<Output>> executingReadOnly;
    private final AtomicBoolean running;
    private Consumer<Exception> handler;

    @SuppressWarnings("unchecked")
    StreamsImplementation(int threads, int tasks, BiFunction<Input, Integer, Integer> partitioner, Class<? extends Deque> queueType, Function<Integer, Input> from, Predicate<Input> filter, Function<Input, Output> process, Consumer<Output> to, BiConsumer<Input, Map<Input, AtomicReference<Output>>> post) {
        this.tasks = tasks;

        if (partitioner != null) {
            this.partitioner = partitioner;
        } else {
            this.partitioner = (input, maximum) -> ThreadLocalRandom.current().nextInt(maximum);
        }

        if (queueType != null) {
            this.queues = new Deque[this.tasks];

            for (int index = 0; index < this.queues.length; index++) {
                try {
                    this.queues[index] = queueType.newInstance();
                } catch (ReflectiveOperationException exception) {
                    throw new RuntimeException(exception);
                }
            }
        } else {
            this.queues = null;
        }

        if (this.queues != null) {
            this.executor = Executors.newFixedThreadPool(threads + 1);
            this.from = (task) -> StreamsImplementation.this.queues[task].poll();
            this.requeue = (task, input) -> StreamsImplementation.this.queues[task].offerFirst(input);
        } else if(from != null) {
            this.executor = Executors.newFixedThreadPool(threads + 1);
            this.from = from;
            this.requeue = null;
        } else {
            this.executor = Executors.newSingleThreadExecutor();
            this.from = null;
            this.requeue = null;
        }

        this.filter = (filter != null)?(filter):(input -> true);
        this.process = (process != null)?(process):(input -> (Output) input);
        this.to = (to != null)?(to):(output -> {});
        this.post = (post != null)?(post):((output, executing) -> {});
        this.executing = new ConcurrentHashMap<>();
        this.executingReadOnly = Collections.unmodifiableMap(this.executing);
        this.running = new AtomicBoolean();
        this.handler = (exception) -> StreamsImplementation.LOG.log(Level.SEVERE, "error inside streams", exception);
    }

    private void process(Input input) {
        if (input != null && this.filter.test(input)) {
            try {
                this.executing.put(input, new AtomicReference<>());

                Output output = this.process.apply(input);

                if (output != null) {
                    this.executing.get(input).set(output);
                    this.to.accept(output);
                    this.post.accept(input, this.executingReadOnly);
                }
            } finally {
                this.executing.remove(input);
            }
        }
    }

    @Override
    public final Streams<Input, Output> start() {
        if ((this.queues != null || this.from != null) && !this.running.getAndSet(true)) {
            Consumer<Integer> consumer = (task) -> {
                Input input = null;

                try {
                    while (this.running.get()) {
                        input = this.from.apply(task);
                        this.process(input);
                        input = null;
                    }
                } catch (Exception exception) {
                    this.executor.submit(() -> this.handler.accept(exception));
                } finally {
                    if (this.requeue != null && input != null) {
                        this.requeue.accept(task, input);
                    }
                }
            };

            for (int index = 0; index < this.tasks; index++) {
                final int task = index;

                this.executor.execute(() -> consumer.accept(task));
            }

            StreamsImplementation.LOG.log(Level.FINE, "streams started");
        }

        return this;
    }

    @Override
    public final Streams<Input, Output> wait(long timeout, TimeUnit unit) throws InterruptedException {
        if (this.running.get()) {
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
        try {
            this.executor.shutdown();
            this.executor.awaitTermination(5L, TimeUnit.SECONDS);
        } finally {
            this.executor.shutdownNow();
        }

        StreamsImplementation.LOG.log(Level.FINE, "streams stopped");
    }

    @Override
    public final void onException(Consumer<Exception> handler) {
        Objects.requireNonNull(handler);
        this.handler = handler;
    }

    @Override
    public final boolean push(Input input) {
        if (this.queues == null && this.from == null) {
            try {
                this.process(input);
                return true;
            } catch (Exception exception) {
                this.handler.accept(exception);
                return false;
            }
        } else {
            Objects.requireNonNull(this.queues, "invalid operation");

            if (!this.running.get()) {
                throw new IllegalStateException("streams is stopped");
            }

            return this.queues[this.partitioner.apply(input, this.tasks)].offer(input);
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
