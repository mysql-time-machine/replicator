package com.booking.replication.applier.console;

import com.booking.replication.applier.EventApplier;
import com.booking.replication.model.Event;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;

public class ConsoleEventApplier implements EventApplier {
    private static final Logger log = LogManager.getLogger(ConsoleEventApplier.class);
    private final ObjectMapper mapper;

    @SuppressWarnings("unused")
    public ConsoleEventApplier(Map<String, String> configuration) {
        this.mapper = new ObjectMapper();
    }

    @Override
    public void accept(Event event) {
        try {
            ConsoleEventApplier.log.info(this.mapper.writeValueAsString(event));
        } catch (JsonProcessingException exception) {
            ConsoleEventApplier.log.error("error converting to json", exception);
        }
    }

    @Override
    public void close() {
    }
}
