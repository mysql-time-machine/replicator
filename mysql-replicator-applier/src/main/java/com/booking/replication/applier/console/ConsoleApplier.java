package com.booking.replication.applier.console;

import com.booking.replication.applier.Applier;
import com.booking.replication.applier.kafka.KafkaApplier;
import com.booking.replication.applier.validation.ValidationService;
import com.booking.replication.augmenter.model.event.AugmentedEvent;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.fasterxml.jackson.databind.ser.impl.SimpleBeanPropertyFilter;
import com.fasterxml.jackson.databind.ser.impl.SimpleFilterProvider;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ConsoleApplier implements Applier {
    private static final Logger LOG = LogManager.getLogger(ConsoleApplier.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private Set<String> includeInColumns = new HashSet<>();

    @SuppressWarnings("unused")
    public ConsoleApplier(Map<String, Object> configuration) {
        this.setupColumnsFilter(configuration);
    }

    private void setupColumnsFilter(Map<String, Object> configuration) {
        this.includeInColumns.add("name");
        this.includeInColumns.add("columnType");

        this.includeInColumns.addAll(this.getAsSet(configuration.get(KafkaApplier.Configuration.INCLUDE_IN_COLUMNS)));

        LOG.info("Adding " + this.includeInColumns.toString() + " fields in metadata.columns.");

        SimpleFilterProvider filterProvider = new SimpleFilterProvider();
        filterProvider.addFilter("column", SimpleBeanPropertyFilter.filterOutAllExcept(this.includeInColumns));
        MAPPER.setFilterProvider(filterProvider);
    }

    @Override
    public Boolean apply(Collection<AugmentedEvent> events) {
        try {
            for (AugmentedEvent event : events) {
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

    @Override
    public ValidationService buildValidationService(Map<String, Object> configuration) {
        return null;
    }

    private Set<String> getAsSet(Object object) {
        if (object != null) {
            if (List.class.isInstance(object)) {
                return new HashSet<>((List<String>) object);
            } else {
                return Collections.singleton(object.toString());
            }
        } else {
            return Collections.emptySet();
        }
    }
}
