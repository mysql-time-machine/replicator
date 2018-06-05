package com.booking.replication.augmenter.model;

@SuppressWarnings("unused")
public class AugmentedEventColumn {
    private String name;
    private String type;
    private boolean nullable;
    private String key;
    private String defaultValue;
    private String extra;

    public AugmentedEventColumn() {
    }

    public AugmentedEventColumn(String name, String type, boolean nullable, String key, String defaultValue, String extra) {
        this.name = name;
        this.type = type;
        this.nullable = nullable;
        this.key = key;
        this.defaultValue = defaultValue;
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

    public String getDefaultValue() {
        return this.defaultValue;
    }

    public String getExtra() {
        return this.extra;
    }
}
