package com.booking.replication.checkpoint;

import com.booking.replication.augmenter.model.event.AugmentedEvent;

import java.util.function.BiConsumer;

public class DummyCheckPointApplier implements CheckpointApplier {
    @Override
    public void close() {

    }

    @Override
    public void accept(AugmentedEvent event, Integer integer) {

    }
}
