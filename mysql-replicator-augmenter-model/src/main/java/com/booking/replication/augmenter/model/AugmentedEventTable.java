package com.booking.replication.augmenter.model;

@SuppressWarnings("unused")
public class AugmentedEventTable {
    private String database;
    private String name;

    public AugmentedEventTable() {
    }

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
