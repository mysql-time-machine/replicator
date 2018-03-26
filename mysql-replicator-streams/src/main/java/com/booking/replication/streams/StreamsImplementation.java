package com.booking.replication.streams;

import java.util.Collections;
import java.util.Deque;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class StreamsImplementation<Input, Output> implements Streams<Input, Output> {
    private static final Logger LOG = Logger.getLogger(StreamsImplementation.class.getName());

    private final int tasks;
    private final Deque<Input> queue;
    private final ExecutorService executor;
    private final Supplier<Input> from;
    private final Predicate<Input> filter;
    private final Function<Input, Output> process;
    private final Consumer<Output> to;
    private final BiConsumer<Input, Map<Input, AtomicReference<Output>>> post;
    private final Map<Input, AtomicReference<Output>> executing;
    private final Map<Input, AtomicReference<Output>> executingReadOnly;
    private final AtomicBoolean running;
    private Consumer<Exception> handler;

    StreamsImplementation(int threads, int tasks, Deque<Input> queue, Supplier<Input> from, Predicate<Input> filter, Function<Input, Output> process, Consumer<Output> to, BiConsumer<Input, Map<Input, AtomicReference<Output>>> post) {
        this.tasks = tasks;
        this.queue = queue;

        if (this.queue != null) {
            this.executor = Executors.newFixedThreadPool(threads);

            if (from == null) {
                this.from = StreamsImplementation.this.queue::poll;
            } else {
                this.from = from;
            }
        } else {
            this.executor = null;
            this.from = null;
        }

        this.filter = filter;
        this.process = process;
        this.to = to;
        this.post = post;
        this.executing = new ConcurrentHashMap<>();
        this.executingReadOnly = Collections.unmodifiableMap(this.executing);
        this.running = new AtomicBoolean();
        this.handler = (exception) -> StreamsImplementation.LOG.log(Level.WARNING, "streams exception handler", exception);
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
        if (this.executor != null && !this.running.getAndSet(true)) {
            Runnable runnable = () -> {
                Input input = null;

                try {
                    while (this.running.get()) {
                        input = this.from.get();
                        this.process(input);
                        input = null;
                    }
                } catch (Exception exception) {
                    this.handler.accept(exception);
                } finally {
                    if (this.queue != null && input != null) {
                        this.queue.offerFirst(input);
                    }
                }
            };

            for (int task = 0; task < this.tasks; task++) {
                this.executor.execute(runnable);
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
        if (this.running.getAndSet(false)) {
            try {
                this.executor.shutdown();
                this.executor.awaitTermination(5L, TimeUnit.SECONDS);
            } finally {
                this.executor.shutdownNow();
            }

            StreamsImplementation.LOG.log(Level.FINE, "streams stopped");
        }
    }

    @Override
    public final void onException(Consumer<Exception> handler) {
        Objects.requireNonNull(handler);
        this.handler = handler;
    }

    @Override
    public final boolean push(Input input) {
        if (this.queue == null && this.from == null) {
            try {
                this.process(input);
                return true;
            } catch (Exception exception) {
                this.handler.accept(exception);
                return false;
            }
        } else {
            Objects.requireNonNull(this.queue, "invalid operation");

            if (!this.running.get()) {
                throw new IllegalStateException("streams is stopped");
            }

            return this.queue.offer(input);
        }
    }

    @Override
    public final int size() {
        if (this.queue != null) {
            return this.queue.size();
        } else {
            return 0;
        }
    }
}
