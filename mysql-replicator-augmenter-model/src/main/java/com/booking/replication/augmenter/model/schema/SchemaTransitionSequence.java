package com.booking.replication.augmenter.model.schema;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public class SchemaTransitionSequence {

    private final String ddl;
    private final Long schemaTransitionTimestamp;
    private final String tableName;

    private final TableSchema tableSchemaBefore;
    private final TableSchema tableSchemaAfter;

    public SchemaTransitionSequence(
            AtomicReference<FullTableName> tableName,
            AtomicReference<Collection<ColumnSchema>> columnsBefore,
            AtomicReference<String> createTableBefore,
            AtomicReference<Collection<ColumnSchema>> columnsAfter,
            AtomicReference<String> createTableAfter,
            String ddl,
            Long schemaTransitionTimestamp) {

        this.tableName = new String(tableName.get().getName());

        this.schemaTransitionTimestamp = new Long(schemaTransitionTimestamp);

        if (columnsBefore.get() != null) { // <- null if table was just created
            this.tableSchemaBefore = new TableSchema(
                    columnsBefore.get().stream().map(c -> c.deepCopy()).collect(Collectors.toList()),
                    new String(createTableBefore.get())
            );
        } else {
            this.tableSchemaBefore = null;
        }

        if (columnsAfter.get() != null) { // <- null if table was dropped
            this.tableSchemaAfter = new TableSchema(
                    columnsAfter.get().stream().map(c -> c.deepCopy()).collect(Collectors.toList()),
                    new String(createTableAfter.get()));
        } else {
            this.tableSchemaAfter = null;
        }

        this.ddl = new String(ddl);
    }

    public String getTableName() {
        return tableName;
    }

    public TableSchema getTableSchemaBefore() {
        return tableSchemaBefore;
    }

    public TableSchema getTableSchemaAfter() {
        return tableSchemaAfter;
    }

    public String getDdl() {
        return ddl;
    }

    public Long getSchemaTransitionTimestamp() {
        return schemaTransitionTimestamp;
    }
}
