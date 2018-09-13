package com.booking.replication.augmenter.model.schema;

import java.io.Serializable;

@SuppressWarnings("unused")
public class FullTableName implements Serializable {

    private String database;
    private String name;

    public FullTableName() {
    }

    public FullTableName(String database, String name) {
        this.database = database;
        this.name = name;
    }

    public String getDatabase() {
        return this.database;
    }

    public String getName() {
        return this.name;
    }

    @Override
    public String toString() {
        return String.format("%s.%s", this.database, this.name);
    }
}
