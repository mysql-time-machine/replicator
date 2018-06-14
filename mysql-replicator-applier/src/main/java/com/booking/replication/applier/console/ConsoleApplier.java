package com.booking.replication.applier.console;

import com.booking.replication.applier.Applier;
import com.booking.replication.augmenter.model.AugmentedEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;

public class ConsoleApplier implements Applier {
    private static final Logger LOG = LogManager.getLogger(ConsoleApplier.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @SuppressWarnings("unused")
    public ConsoleApplier(Map<String, Object> configuration) {
    }

    @Override
    public void accept(AugmentedEvent augmentedEvent) {
        try {
            ConsoleApplier.LOG.info(ConsoleApplier.MAPPER.writeValueAsString(augmentedEvent));
        } catch (JsonProcessingException exception) {
            ConsoleApplier.LOG.error("error converting to json", exception);
        }
    }
}
