package com.booking.replication.augmenter.model;

import java.io.Serializable;

@SuppressWarnings("unused")
public class AugmentedEventColumn implements Serializable {
    private String name;
    private String type;
    private boolean nullable;
    private String key;
    private String valueDefault;
    private String extra;

    public AugmentedEventColumn() {
    }

    public AugmentedEventColumn(String name, String type, boolean nullable, String key, String valueDefault, String extra) {
        this.name = name;
        this.type = type;
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
}
