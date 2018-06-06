package com.booking.replication.streams;

import org.junit.Test;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class StreamsTest {
    @Test
    public void testFromPull() throws InterruptedException {
        AtomicInteger inputCount = new AtomicInteger();
        AtomicInteger outputCount = new AtomicInteger();

        Streams.<Integer>builder()
                .queue()
                .fromPull((task) -> {
                    inputCount.incrementAndGet();
                    return ThreadLocalRandom.current().nextInt();
                })
                .to((value) -> assertEquals(inputCount.get(), outputCount.incrementAndGet()))
                .build()
                .start()
                .wait(1L, TimeUnit.SECONDS)
                .stop();
    }

    @Test
    public void testFromPush() throws InterruptedException {
        AtomicInteger count = new AtomicInteger();

        int number = ThreadLocalRandom.current().nextInt();

        Streams<Integer, Integer> streams = Streams.<Integer>builder()
                .queue()
                .fromPush()
                .to((value) -> {
                    assertEquals(1, count.incrementAndGet());
                    assertEquals(number, value.intValue());
                })
                .build()
                .start();

        streams.push(number);
        streams.wait(1L, TimeUnit.SECONDS).stop();
    }

    @Test
    public void testFilter() throws InterruptedException {
        AtomicInteger inputCount = new AtomicInteger();
        AtomicInteger outputCount = new AtomicInteger();

        Streams.<Integer>builder()
                .queue()
                .fromPull((task) -> {
                    int value = ThreadLocalRandom.current().nextInt();

                    if (value > 0) {
                        inputCount.incrementAndGet();
                    }

                    return value;
                })
                .filter((value) -> value > 0)
                .to((value) -> {
                    assertEquals(inputCount.get(), outputCount.incrementAndGet());
                    assertTrue(value > 0);
                })
                .build()
                .start()
                .wait(1L, TimeUnit.SECONDS)
                .stop();
    }

    @Test
    public void testProcess() throws InterruptedException {
        AtomicInteger inputCount = new AtomicInteger();
        AtomicInteger outputCount = new AtomicInteger();

        Streams.<Integer>builder()
                .queue()
                .fromPull((task) -> {
                    inputCount.incrementAndGet();
                    return ThreadLocalRandom.current().nextInt();
                })
                .process(Object::toString)
                .to((value) -> {
                    assertEquals(inputCount.get(), outputCount.incrementAndGet());
                    assertTrue(String.class.isInstance(value));
                })
                .build()
                .start()
                .wait(1L, TimeUnit.SECONDS)
                .stop();
    }

    @Test
    public void testMultipleProcess() throws InterruptedException {
        AtomicInteger inputCount = new AtomicInteger();
        AtomicInteger outputCount = new AtomicInteger();

        Streams.<Integer>builder()
                .queue()
                .fromPull((task) -> {
                    inputCount.incrementAndGet();
                    return ThreadLocalRandom.current().nextInt();
                })
                .process(Object::toString)
                .process((value) -> String.format("value=%s", value))
                .to((value) -> {
                    assertEquals(inputCount.get(), outputCount.incrementAndGet());
                    assertTrue(value.startsWith("value="));
                })
                .build()
                .start()
                .wait(1L, TimeUnit.SECONDS)
                .stop();
    }

    @Test
    public void testThreads() throws InterruptedException {
        AtomicInteger inputCount = new AtomicInteger();
        AtomicInteger outputCount = new AtomicInteger();

        Streams.<Integer>builder()
                .tasks(10)
                .threads(10)
                .queue()
                .fromPull((task) -> {
                    inputCount.incrementAndGet();
                    return ThreadLocalRandom.current().nextInt();
                })
                .process(Object::toString)
                .process((value) -> String.format("value=%s", value))
                .to((value) -> assertEquals(inputCount.get(), outputCount.incrementAndGet()))
                .build()
                .start()
                .wait(1L, TimeUnit.SECONDS)
                .stop();
    }

    @Test
    public void testMultipleTo() throws InterruptedException {
        AtomicInteger inputCount = new AtomicInteger();
        AtomicInteger outputCount1 = new AtomicInteger();
        AtomicInteger outputCount2 = new AtomicInteger();

        Streams.<Integer>builder()
                .queue()
                .fromPull((task) -> {
                    int value = ThreadLocalRandom.current().nextInt();

                    if (value > 0) {
                        inputCount.incrementAndGet();
                    }

                    return value;
                })
                .filter(value -> value > 0)
                .process(Object::toString)
                .to((value) -> {
                    assertEquals(inputCount.get(), outputCount1.incrementAndGet());
                    assertTrue(String.class.isInstance(value));
                })
                .to((value) -> {
                    assertEquals(inputCount.get(), outputCount2.incrementAndGet());
                    assertTrue(Integer.parseInt(value) > 0);
                })
                .build()
                .start()
                .wait(1L, TimeUnit.SECONDS)
                .stop();
    }

    @Test
    public void testPost() throws InterruptedException {
        AtomicInteger inputCount = new AtomicInteger();
        AtomicInteger outputCount1 = new AtomicInteger();
        AtomicInteger outputCount2 = new AtomicInteger();

        Streams.<Integer>builder()
                .queue()
                .fromPull((task) -> {
                    int value = ThreadLocalRandom.current().nextInt();

                    if (value > 0) {
                        inputCount.incrementAndGet();
                    }

                    return value;
                })
                .filter(value -> value > 0)
                .process(Object::toString)
                .to((value) -> {
                    assertEquals(inputCount.get(), outputCount1.incrementAndGet());
                    assertTrue(String.class.isInstance(value));
                })
                .post((value) -> {
                    assertEquals(inputCount.get(), outputCount2.incrementAndGet());
                    assertTrue(value > 0);
                })
                .build()
                .start()
                .wait(1L, TimeUnit.SECONDS)
                .stop();
    }

    @Test
    public void testOnException() throws InterruptedException {
        int number = ThreadLocalRandom.current().nextInt();

        Streams<Integer, Integer> streams = Streams.<Integer>builder()
                .queue()
                .fromPush()
                .to((value) -> {
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
        AtomicInteger inputCount = new AtomicInteger();
        AtomicInteger outputCount1 = new AtomicInteger();
        AtomicInteger outputCount2 = new AtomicInteger();
        AtomicInteger outputCount3 = new AtomicInteger();

        Streams<String, String> streamsDestination = Streams.<String>builder()
                .tasks(10)
                .threads(10)
                .queue()
                .fromPush()
                .to(output -> assertEquals(outputCount1.get(), outputCount2.incrementAndGet()))
                .post(input -> assertEquals(outputCount2.get(), outputCount3.incrementAndGet()))
                .build();

        Streams<Integer, String> streamsSource = Streams.<Integer>builder()
                .fromPush()
                .process(Object::toString)
                .process((value) -> {
                    assertEquals(inputCount.get(), outputCount1.incrementAndGet());
                    return String.format("value=%s", value);
                })
                .to(streamsDestination::push)
                .build();

        streamsDestination.start();
        streamsSource.start();

        for (int index = 0; index < 20; index++) {
            inputCount.incrementAndGet();
            streamsSource.push(ThreadLocalRandom.current().nextInt());
        }

        streamsSource.wait(1L, TimeUnit.SECONDS).stop();
    }
}
