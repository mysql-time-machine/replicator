package com.booking.replication.model.augmented.active.schema;

/**
 * Created by smalviya on 1/24/18.
 */
@SuppressWarnings("unused")
public interface ColumnSchema {
    String getColumnKey();
    String getCharacterSetName();
    String getDataType();
    int getCharacterMaximumLength();
    boolean isNullable();
    String getColumnName();
    int getOrdinalPosition();
    String getColumnType();
}
