package com.booking.replication.applier.console;

import com.booking.replication.applier.EventApplier;
import com.booking.replication.mysql.binlog.model.Event;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ConsoleEventApplier implements EventApplier {
    private static final Logger log = Logger.getLogger(ConsoleEventApplier.class.getName());
    private final ObjectMapper mapper;

    @SuppressWarnings("unused")
    public ConsoleEventApplier(Map<String, String> configuration) {
        this.mapper = new ObjectMapper();
    }

    @Override
    public void accept(Event event) {
        try {
            ConsoleEventApplier.log.log(Level.INFO, this.mapper.writeValueAsString(event));
        } catch (JsonProcessingException exception) {
            ConsoleEventApplier.log.log(Level.SEVERE, "error converting to json", exception);
        }
    }

    @Override
    public void close() {
    }
}
