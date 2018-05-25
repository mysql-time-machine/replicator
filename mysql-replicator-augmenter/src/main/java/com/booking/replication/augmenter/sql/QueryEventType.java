package com.booking.replication.augmenter.sql;

public enum QueryEventType {
    BEGIN,
    COMMIT,
    DDLDEFINER,
    DDLTABLE,
    DDLTEMPORARYTABLE,
    DDLVIEW,
    PSEUDOGTID,
    ANALYZE,
    UNKNOWN;
    QueryEventType() {
    }
}