package com.booking.replication.augmenter;

import com.booking.replication.augmenter.model.schema.ColumnSchema;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

public interface SchemaManager extends Closeable {
    boolean execute(String tableName, String query);

    List<ColumnSchema> listColumns(String tableName);

    String getCreateTable(String tableName);

    @Override
    void close() throws IOException;
}
