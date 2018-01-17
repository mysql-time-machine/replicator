package com.booking.replication.streams;

import java.util.Objects;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class StreamsImplementation<Input, Output> implements Streams<Input, Output> {
    private static final Logger log = Logger.getLogger(StreamsImplementation.class.getName());

    private final ExecutorService executor;
    private final int tasks;
    private final BlockingDeque<Input> queue;
    private final Supplier<Input> from;
    private final Predicate<Input> filter;
    private final Function<Input, Output> process;
    private final Consumer<Output> to;
    private final Consumer<Input> post;
    private final AtomicBoolean running;
    private Consumer<Exception> handler;

    StreamsImplementation(int threads, int tasks, Supplier<Input> from, Predicate<Input> filter, Function<Input, Output> process, Consumer<Output> to, Consumer<Input> post) {
        this.executor = Executors.newFixedThreadPool(threads);
        this.tasks = tasks;

        if (from == null) {
            this.queue = new LinkedBlockingDeque<>();
            this.from = () -> {
                try {
                    return StreamsImplementation.this.queue.poll(1L, TimeUnit.MINUTES);
                } catch (InterruptedException exception) {
                    return null;
                }
            };
        } else {
            this.queue = null;
            this.from = from;
        }

        this.filter = filter;
        this.process = process;
        this.to = to;
        this.post = post;
        this.running = new AtomicBoolean();
        this.handler = (exception) -> StreamsImplementation.log.log(Level.WARNING, "streams exception handler", exception);
    }

    @Override
    public final Streams<Input, Output> start() {
        if (!this.running.getAndSet(true)) {
            Runnable runnable = () -> {
                Input input = null;

                try {
                    while (this.running.get()) {
                        input = this.from.get();

                        if (input != null && this.filter.test(input)) {
                            Output output = this.process.apply(input);

                            if (output != null) {
                                this.to.accept(this.process.apply(input));
                                this.post.accept(input);
                            }
                        }

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

            StreamsImplementation.log.log(Level.FINE, "streams started");
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

            StreamsImplementation.log.log(Level.FINE, "streams stopped");
        }
    }

    @Override
    public final void onException(Consumer<Exception> handler) {
        Objects.requireNonNull(handler);
        this.handler = handler;
    }

    @Override
    public final boolean push(Input input) {
        Objects.requireNonNull(this.queue, "invalid operation");

        if (!this.running.get()) {
            throw new IllegalStateException("streams is stopped");
        }

        return this.queue.offer(input);
    }
}
