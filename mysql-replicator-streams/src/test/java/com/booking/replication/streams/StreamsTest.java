package com.booking.replication.streams;

import org.junit.Test;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class StreamsTest {
    @Test
    public void testPullMode() throws InterruptedException {
        AtomicInteger count1 = new AtomicInteger();
        AtomicInteger count2 = new AtomicInteger();

        Streams<Integer, Integer> streams  = Streams.<Integer>builder()
                .useDefaultQueueType()
                .setDataSupplier((task) -> {
                    count1.incrementAndGet();
                    return ThreadLocalRandom.current().nextInt();
                })
                .setSink((value) -> {
                    count2.incrementAndGet();
                    return true;
                }).build();

        streams.start()
               .wait(1L, TimeUnit.SECONDS)
               .stop();

        assertEquals(count1.get(), count2.get());
    }

    @Test
    public void testPushMode() throws InterruptedException {
        AtomicInteger count1 = new AtomicInteger();

        int number = ThreadLocalRandom.current().nextInt();

        Streams<Integer, Integer> streams = Streams.<Integer>builder()
                .useDefaultQueueType()
                .usePushMode()
                .setSink((value) -> {
                    count1.incrementAndGet();
                    assertEquals(number, value.intValue());
                    return true;
                })
                .build()
                .start();

        streams.push(number);
        streams.wait(1L, TimeUnit.SECONDS).stop();

        assertEquals(1, count1.get());
    }

    @Test
    public void testFilter() throws InterruptedException {
        AtomicInteger count1 = new AtomicInteger();
        AtomicInteger count2 = new AtomicInteger();

        Streams.<Integer>builder()
                .useDefaultQueueType()
                .setDataSupplier((task) -> {
                    int value = ThreadLocalRandom.current().nextInt();

                    if (value > 0) {
                        count1.incrementAndGet();
                    }

                    return value;
                })
                .filter((value) -> value > 0)
                .setSink((value) -> {
                    count2.incrementAndGet();
                    assertTrue(value > 0);
                    return true;
                })
                .build()
                .start()
                .wait(1L, TimeUnit.SECONDS)
                .stop();

        assertEquals(count1.get(), count2.get());
    }

    @Test
    public void testProcess() throws InterruptedException {
        AtomicInteger count1 = new AtomicInteger();
        AtomicInteger count2 = new AtomicInteger();

        Streams.<Integer>builder()
                .useDefaultQueueType()
                .setDataSupplier((task) -> {
                    count1.incrementAndGet();
                    return ThreadLocalRandom.current().nextInt();
                })
                .process(Object::toString)
                .setSink((value) -> {
                    count2.incrementAndGet();
                    assertTrue(String.class.isInstance(value));
                    return true;
                })
                .build()
                .start()
                .wait(1L, TimeUnit.SECONDS)
                .stop();

        assertEquals(count1.get(), count2.get());
    }

    @Test
    public void testMultipleProcess() throws InterruptedException {
        AtomicInteger count1 = new AtomicInteger();
        AtomicInteger count2 = new AtomicInteger();

        Streams.<Integer>builder()
                .useDefaultQueueType()
                .setDataSupplier((task) -> {
                    count1.incrementAndGet();
                    return ThreadLocalRandom.current().nextInt();
                })
                .process(Object::toString)
                .process((value) -> String.format("value=%s", value))
                .setSink((value) -> {
                    count2.incrementAndGet();
                    assertTrue(value.startsWith("value="));
                    return true;
                })
                .build()
                .start()
                .wait(1L, TimeUnit.SECONDS)
                .stop();

        assertEquals(count1.get(), count2.get());
    }

    @Test
    public void testThreads() throws InterruptedException {
        AtomicInteger count1 = new AtomicInteger();
        AtomicInteger count2 = new AtomicInteger();

        Streams<Integer, String> streams = Streams.<Integer>builder()
                .tasks(10)
                .threads(10)
                .useDefaultQueueType()
                .setDataSupplier((task) -> {
                    count1.incrementAndGet();
                    return ThreadLocalRandom.current().nextInt();
                })
                .process(Object::toString)
                .process((value) -> String.format("value=%s", value))
                .setSink((value) -> {
                    count2.incrementAndGet();
                    return true;
                })
                .build()
                .start();

        while (count2.get() < count1.get()) {
            streams.wait(1L, TimeUnit.SECONDS);
        }

        streams.stop();

        assertEquals(count1.get(), count2.get());
    }

    @Test
    public void testMultipleTo() throws InterruptedException {
        AtomicInteger count1 = new AtomicInteger();
        AtomicInteger count2 = new AtomicInteger();
        AtomicInteger count3 = new AtomicInteger();

        Streams.<Integer>builder()
                .useDefaultQueueType()
                .setDataSupplier((task) -> {
                    int value = ThreadLocalRandom.current().nextInt();

                    if (value > 0) {
                        count1.incrementAndGet();
                    }

                    return value;
                })
                .filter(value -> value > 0)
                .process(Object::toString)
                .setSink((value) -> {
                    count2.incrementAndGet();
                    assertTrue(String.class.isInstance(value));
                    return true;
                })
                .setSink((value) -> {
                    count3.incrementAndGet();
                    assertTrue(Integer.parseInt(value) > 0);
                    return true;
                })
                .build()
                .start()
                .wait(1L, TimeUnit.SECONDS)
                .stop();

        assertEquals(count1.get(), count2.get());
        assertEquals(count1.get(), count3.get());
    }

    @Test
    public void testPost() throws InterruptedException {
        AtomicInteger count1 = new AtomicInteger();
        AtomicInteger count2 = new AtomicInteger();
        AtomicInteger count3 = new AtomicInteger();

        Streams.<Integer>builder()
                .useDefaultQueueType()
                .setDataSupplier((task) -> {
                    int value = ThreadLocalRandom.current().nextInt();

                    if (value > 0) {
                        count1.incrementAndGet();
                    }

                    return value;
                })
                .filter(value -> value > 0)
                .process(Object::toString)
                .setSink((value) -> {
                    count2.incrementAndGet();
                    assertTrue(String.class.isInstance(value));
                    return true;
                })
                .post((value) -> {
                    count3.incrementAndGet();
                    assertTrue(value > 0);
                })
                .build()
                .start()
                .wait(1L, TimeUnit.SECONDS)
                .stop();

        assertEquals(count1.get(), count2.get());
        assertEquals(count1.get(), count3.get());
    }

    @Test
    public void testOnException() throws InterruptedException {
        int number = ThreadLocalRandom.current().nextInt();

        Streams<Integer, Integer> streams = Streams.<Integer>builder()
                .useDefaultQueueType()
                .usePushMode()
                .setSink((value) -> {
                    throw new NullPointerException();
                })
                .build()
                .start();

        streams.onException((exception) -> assertTrue(NullPointerException.class.isInstance(exception)));
        streams.push(number);
        streams.wait(1L, TimeUnit.SECONDS).stop();
    }

    @Test
    public void testChain() throws InterruptedException {
        AtomicInteger count1 = new AtomicInteger();
        AtomicInteger count2 = new AtomicInteger();
        AtomicInteger count3 = new AtomicInteger();
        AtomicInteger count4 = new AtomicInteger();

        Streams<String, String> streamsDestination = Streams.<String>builder()
                .tasks(10)
                .threads(10)
                .useDefaultQueueType()
                .usePushMode()
                .setSink(output -> {
                    count3.incrementAndGet();
                    return true;
                })
                .post(input -> count4.incrementAndGet())
                .build();

        Streams<Integer, String> streamsSource = Streams.<Integer>builder()
                .usePushMode()
                .process(Object::toString)
                .process((value) -> {
                    count2.incrementAndGet();
                    return String.format("value=%s", value);
                })
                .setSink(s -> {
                    streamsDestination.push(s);
                    return true;
                })
                .build();

        streamsDestination.start();
        streamsSource.start();

        for (int index = 0; index < 20; index++) {
            count1.incrementAndGet();
            streamsSource.push(ThreadLocalRandom.current().nextInt());
        }

        while (count4.get() < count1.get()) {
            streamsSource.wait(1L, TimeUnit.SECONDS);
        }

        streamsDestination.stop();
        streamsSource.stop();

        assertEquals(count1.get(), count2.get());
        assertEquals(count1.get(), count3.get());
        assertEquals(count1.get(), count4.get());
    }
}
