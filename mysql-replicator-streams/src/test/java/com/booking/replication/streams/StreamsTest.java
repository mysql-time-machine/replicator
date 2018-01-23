package com.booking.replication.streams;

import org.junit.Test;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class StreamsTest {
    @Test
    public void testFromPull() throws InterruptedException {
        Streams.<Integer>builder()
                .fromPull(() -> ThreadLocalRandom.current().nextInt())
                .to((value) -> {})
                .build()
                .start()
                .wait(1L, TimeUnit.SECONDS)
                .stop();
    }

    @Test
    public void testFromPush() throws InterruptedException  {
        int number = ThreadLocalRandom.current().nextInt();

        Streams<Integer, Integer> streams = Streams.<Integer>builder()
                .fromPush()
                .to((value) -> assertEquals(number, value.intValue()))
                .build()
                .start();

        streams.push(number);
        streams.wait(1L, TimeUnit.SECONDS).stop();
    }

    @Test
    public void testFilter() throws InterruptedException  {
        Streams.<Integer>builder()
                .fromPull(() -> ThreadLocalRandom.current().nextInt())
                .filter((value) -> value > 0)
                .to((value) -> assertTrue(value > 0))
                .build()
                .start()
                .wait(1L, TimeUnit.SECONDS)
                .stop();
    }

    @Test
    public void testProcess() throws InterruptedException {
        Streams.<Integer>builder()
                .fromPull(() -> ThreadLocalRandom.current().nextInt())
                .filter((value) -> value > 0)
                .process(Object::toString)
                .to((value) -> assertTrue(String.class.isInstance(value)))
                .build()
                .start()
                .wait(1L, TimeUnit.SECONDS)
                .stop();
    }

    @Test
    public void testMultipleProcess() throws InterruptedException {
        Streams.<Integer>builder()
                .fromPull(() -> ThreadLocalRandom.current().nextInt())
                .filter(value -> value > 0)
                .process(Object::toString)
                .process((value) -> String.format("value=%s", value))
                .to((value) -> assertTrue(value.startsWith("value=")))
                .build()
                .start()
                .wait(1L, TimeUnit.SECONDS)
                .stop();
    }

    @Test
    public void testMultipleTo() throws InterruptedException {
        Streams.<Integer>builder()
                .fromPull(() -> ThreadLocalRandom.current().nextInt())
                .filter(value -> value > 0)
                .process(Object::toString)
                .to((value) -> assertTrue(String.class.isInstance(value)))
                .to((value) -> assertTrue(Integer.parseInt(value) > 0))
                .build()
                .start()
                .wait(1L, TimeUnit.SECONDS)
                .stop();
    }

    @Test
    public void testPost() throws InterruptedException {
        Streams.<Integer>builder()
                .fromPull(() -> ThreadLocalRandom.current().nextInt())
                .filter(value -> value > 0)
                .process(Object::toString)
                .to((value) -> assertTrue(String.class.isInstance(value)))
                .post((value) -> assertTrue(value > 0))
                .build()
                .start()
                .wait(1L, TimeUnit.SECONDS)
                .stop();
    }

    @Test
    public void testOnException() throws InterruptedException {
        int number = ThreadLocalRandom.current().nextInt();

        Streams<Integer, Integer> streams = Streams.<Integer>builder()
                .fromPush()
                .to((value) -> { throw new NullPointerException(); })
                .build()
                .start();

        streams.onException((exception) -> assertTrue(NullPointerException.class.isInstance(exception)));
        streams.push(number);
        streams.wait(1L, TimeUnit.SECONDS).stop();
    }
}
