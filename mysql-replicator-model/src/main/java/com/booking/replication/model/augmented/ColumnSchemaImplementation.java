package com.booking.replication.model.augmented;

import com.booking.replication.model.augmented.ColumnSchema;

public class ColumnSchemaImplementation implements ColumnSchema {
    private String columnKey;
    private String characterSetName;
    private String dataType;
    private int characterMaximumLength;
    private boolean nullable;
    private String columnName;
    private int ordinalPosition;
    private String columnType;

    @Override
    public String getColumnKey() {
        return this.columnKey;
    }

    public void setColumnKey(String columnKey) {
        this.columnKey = columnKey;
    }

    @Override
    public String getCharacterSetName() {
        return this.characterSetName;
    }

    public void setCharacterSetName(String characterSetName) {
        this.characterSetName = characterSetName;
    }

    @Override
    public String getDataType() {
        return this.dataType;
    }

    public void setDataType(String dataType) {
        this.dataType = dataType;
    }

    @Override
    public int getCharacterMaximumLength() {
        return this.characterMaximumLength;
    }

    public void setCharacterMaximunLength(int characterMaximumLength) {
        this.characterMaximumLength = characterMaximumLength;
    }

    @Override
    public boolean isNullable() {
        return this.nullable;
    }

    public void setNullable(boolean nullable) {
        this.nullable = nullable;
    }

    @Override
    public String getColumnName() {
        return this.columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    @Override
    public int getOrdinalPosition() {
        return this.ordinalPosition;
    }

    public void setOrdinalPosition(int ordinalPosition) {
        this.ordinalPosition = ordinalPosition;
    }

    @Override
    public String getColumnType() {
        return this.columnType;
    }

    public void setColumnType(String columnType) {
        this.columnType = columnType;
    }
}
