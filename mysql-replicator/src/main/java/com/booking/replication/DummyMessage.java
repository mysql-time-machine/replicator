package com.booking.replication;

import java.util.UUID;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.ObjectIdGenerators;

import java.util.UUID;

public class DummyMessage {

        private Long timestamp;
        private Long ordinal;
        private String uuid;

        public DummyMessage(Long eventCounter, Long eventTimestamp, String uuidString) {

            timestamp = eventTimestamp;
            ordinal = eventCounter;

            UUID uuidObject = UUID.randomUUID();
            uuid = uuidString;

        }

        public Long getTimestamp() {
            return timestamp;
        }

        public Long getOrdinal() {
            return ordinal;
        }

        public String getUuid() {
            return uuid;
        }
    }

