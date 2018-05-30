package com.booking.replication.applier.console;

import com.booking.replication.applier.EventApplier;
import com.booking.replication.augmenter.model.AugmentedEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;

public class ConsoleEventApplier implements EventApplier {
    private static final Logger LOG = LogManager.getLogger(ConsoleEventApplier.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @SuppressWarnings("unused")
    public ConsoleEventApplier(Map<String, String> configuration) {
    }

    @Override
    public void accept(AugmentedEvent augmentedEvent) {
        try {
            ConsoleEventApplier.LOG.info(ConsoleEventApplier.MAPPER.writeValueAsString(augmentedEvent));
        } catch (JsonProcessingException exception) {
            ConsoleEventApplier.LOG.error("error converting to json", exception);
        }
    }

    @Override
    public void close() {
    }
}
