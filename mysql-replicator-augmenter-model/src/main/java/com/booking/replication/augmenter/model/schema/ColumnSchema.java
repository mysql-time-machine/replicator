package com.booking.replication.augmenter.model.schema;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.io.Serializable;

@SuppressWarnings("unused")
public class ColumnSchema implements Serializable {
    private String name;
    private String type;
    private boolean nullable;
    private String key;
    private String valueDefault;
    private String extra;
    private String collation;

    @JsonIgnore
    public boolean primary; // temp transition

    public ColumnSchema() {
    }

    public ColumnSchema(
        String name,
        String type,
        String collation,
        boolean nullable,
        String key,
        String valueDefault,
        String extra
    ) {
        this.name = name;
        this.type = type;
        this.collation = collation;
        this.nullable = nullable;
        this.key = key;
        this.valueDefault = valueDefault;
        this.extra = extra;
    }


    public String getName() {
        return this.name;
    }

    public String getType() {
        return this.type;
    }

    public String getCollation() {
        return collation;
    }

    public boolean isNullable() {
        return this.nullable;
    }

    public String getKey() {
        return this.key;
    }

    public String getValueDefault() {
        return this.valueDefault;
    }

    public String getExtra() {
        return this.extra;
    }

    public ColumnSchema deepCopy() {

        String name = this.getName();
        String type = this.getType();
        String collation = this.getCollation();
        boolean nullable = this.isNullable();
        String key = this.getKey();
        String valueDefault = (this.getValueDefault() == null) ? "NULL" : this.getValueDefault();
        String extra = this.getExtra();

        ColumnSchema columnSchemaCopy = new ColumnSchema(
            name, type, collation, nullable, key, valueDefault, extra
        );

        return columnSchemaCopy;
    }

    public boolean isPrimary() {
        return key.equalsIgnoreCase("PRI");
    }
}
