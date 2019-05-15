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
        this.name = this.cleaned(name);
    }

    private String cleaned(String name) {
        if(name == null) return name;
        name = name.replaceAll("`", "");
        if ( name.contains(".") ) {
            return name.split("\\.")[1];
        }
        return name;
    }

    public String getDatabase() {
        return this.database;
    }

    public String getName() {
        return this.name;
    }

    public void setName(String tableName) {
        this.name = tableName;
    }

    @Override
    public String toString() {
        return String.format("%s.%s", this.database, this.name);
    }
}
