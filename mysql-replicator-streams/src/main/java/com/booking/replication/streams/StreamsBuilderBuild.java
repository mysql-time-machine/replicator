package com.booking.replication.streams;

public interface StreamsBuilderBuild<Input, Output> {
    Streams<Input, Output> build();
}
