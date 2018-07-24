package com.booking.replication.applier.hbase;

import com.booking.replication.applier.Applier;
import com.booking.replication.augmenter.model.AugmentedEvent;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;

public class HBaseApplier implements Applier {

    private static final Logger LOG = LogManager.getLogger(com.booking.replication.applier.hbase.HBaseApplier.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @SuppressWarnings("unused")
    public HBaseApplier(Map<String, Object> configuration) {
    }

    @Override
    public void accept(AugmentedEvent augmentedEvent) {
        try {
            com.booking.replication.applier.hbase.HBaseApplier.LOG.info(
                    com.booking.replication.applier.hbase.HBaseApplier.MAPPER.writeValueAsString(augmentedEvent)
            );
        } catch (JsonProcessingException exception) {
            com.booking.replication.applier.hbase.HBaseApplier.LOG.error("error converting to json", exception);
        }
    }
}
