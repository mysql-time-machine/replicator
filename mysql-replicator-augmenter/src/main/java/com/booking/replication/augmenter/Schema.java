package com.booking.replication.augmenter;

import com.booking.replication.augmenter.model.AugmentedEventColumn;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

public interface Schema extends Closeable {
    boolean execute(String tableName, String query);

    List<AugmentedEventColumn> listColumns(String tableName);

    String getCreateTable(String tableName);

    @Override
    void close() throws IOException;
}
