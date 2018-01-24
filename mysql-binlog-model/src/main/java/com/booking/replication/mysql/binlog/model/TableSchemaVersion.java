package com.booking.replication.mysql.binlog.model;

import java.util.Collections;
import java.util.Set;
import java.util.UUID;


public interface TableSchemaVersion {


    public void addColumn();

    public ColumnSchema getColumnSchemaByColumnName(String columnName);

    public ColumnSchema getColumnSchemaByColumnIndex(Integer columnIndex);

    public Set<String> getColumnNames();
}
