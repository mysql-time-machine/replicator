package com.booking.replication.mysql.binlog.model;

/**
 * Created by smalviya on 1/24/18.
 */
public interface ColumnSchema {
    public String getColumnKey();

    public void setColumnKey(String columnKey);

    public String getCharacterSetName();

    public void setCharacterSetName(String characterSetName);

    public String getDataType();

    public void setDataType(String dataType);

    public int getCharacterMaximumLength();

    public void setCharacterMaximumLength(int characterMaximumLength);

    public boolean isNullable();

    public void setNullable(boolean nullable);

    public String getColumnName();

    public void setColumnName(String columnName);

    public int getOrdinalPosition();

    public void setOrdinalPosition(int ordinalPosition);

    public String getColumnType();

    public void setColumnType(String columnType);
}
