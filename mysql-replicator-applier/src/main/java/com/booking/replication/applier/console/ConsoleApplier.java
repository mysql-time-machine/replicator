package com.booking.replication.applier.console;

import com.booking.replication.applier.Applier;
import com.booking.replication.augmenter.model.event.AugmentedEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collection;
import java.util.Map;

public class ConsoleApplier implements Applier {
    private static final Logger LOG = LogManager.getLogger(ConsoleApplier.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @SuppressWarnings("unused")
    public ConsoleApplier(Map<String, Object> configuration) {
    }

    @Override
    public Boolean apply(Collection<AugmentedEvent> events) {
        try {
            for (AugmentedEvent event : events) {
//                ConsoleApplier.LOG.info(ConsoleApplier.MAPPER.writeValueAsString(event));
                ConsoleApplier.LOG.info(ConsoleApplier.MAPPER.writeValueAsString(event));
            }

            return true;
        } catch (JsonProcessingException exception) {
            ConsoleApplier.LOG.error("error converting to json", exception);

            return false;
        }
    }

    @Override
    public boolean forceFlush() {
        return false;
    }
}
