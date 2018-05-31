package com.booking.replication.augmenter.model;

public class AugmentedEventTable {
    private final String database;
    private final String name;

    public AugmentedEventTable(String database, String name) {
        this.database = database;
        this.name = name;
    }

    public String getDatabase() {
        return this.database;
    }

    public String getName() {
        return this.name;
    }
}
