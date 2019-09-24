package com.booking.replication.augmenter.model.schema;

import java.io.Serializable;

@SuppressWarnings("unused")
public class FullTableName implements Serializable {

    private String db;
    private String name;

    public FullTableName() { }

    public FullTableName(String db, String fullName) {
        this.db     = db;
        this.name   = this.getTableName(fullName);
    }

    private String getTableName(String fullName) {
        if (fullName == null) {
            return null;
        }

        fullName = fullName.replaceAll("`", "");

        if ( fullName.contains(".") ) {
            return fullName.split("\\.")[1];
        }

        return fullName;
    }

    public String getDb() {
        return this.db;
    }

    public String getName() {
        return this.name;
    }

    public void setName(String tableName) {
        this.name = tableName;
    }

    @Override
    public String toString() {
        return String.format("%s.%s", this.db, this.name);
    }
}
