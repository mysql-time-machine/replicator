package com.booking.replication.augmenter.model.schema;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;

public class SchemaAtPositionCache {

    private final Map<Long, FullTableName> tableIdToTableNameMap;
    private final Map<String, TableSchema> tableSchemaCache;

    public SchemaAtPositionCache() {
        this.tableIdToTableNameMap = new ConcurrentHashMap<>();
        this.tableSchemaCache = new ConcurrentHashMap<>();
    }

    public Map<Long, FullTableName> getTableIdToTableNameMap() {
        return tableIdToTableNameMap;
    }

    public void removeTableFromCache(String tableName) {
        this.tableSchemaCache.remove(tableName);
    }

    public Map<String, TableSchema> getTableSchemaCache() {
        return tableSchemaCache;
    }

    public Map<String, String> getCreateTableStatements() {
        Map<String, String> tableCreateStatements = this
            .tableSchemaCache
            .entrySet()
            .stream()
            .collect(
                Collectors.toMap(entry -> entry.getKey(), entry -> entry.getValue().getCreate()));
        return tableCreateStatements;
    }

    // get from tableSchemaCache or from active schema
    public TableSchema getTableColumns(
        String tableName,
        Function<String, TableSchema> computeAndReturnTableSchema) {
        return this.tableSchemaCache.computeIfAbsent(tableName, computeAndReturnTableSchema);
    }

    public void reloadTableSchema(
        String tableName,
        Function<String, TableSchema> computeAndReturnTableSchema) {
        this.tableSchemaCache.computeIfAbsent(tableName, computeAndReturnTableSchema);
    }

    public SchemaAtPositionCache deepCopy() {

        SchemaAtPositionCache deepCopy = new SchemaAtPositionCache();

        for (String tableName : this.getTableSchemaCache().keySet()) {

            TableSchema tableSchema = this.getTableSchemaCache().get(tableName);

            List<ColumnSchema> clonedColumnSchemaList = new ArrayList<>();

            String create = tableSchema.getCreate();

            FullTableName fullTableNameCloned = new FullTableName(
                tableSchema.getFullTableName().getDatabase(),
                tableSchema.getFullTableName().getName()
            );

            for (ColumnSchema columnSchema : tableSchema.getColumnSchemas()) {
                ColumnSchema columnSchemaCopy = columnSchema.deepCopy();
                clonedColumnSchemaList.add(columnSchemaCopy);
            }

            TableSchema tableSchemaClone = new TableSchema(fullTableNameCloned, clonedColumnSchemaList, create);

            deepCopy.getTableSchemaCache().put(tableName, tableSchemaClone);
        }

        for (Map.Entry<Long, FullTableName> entry : tableIdToTableNameMap.entrySet()) {
            deepCopy.getTableIdToTableNameMap()
                .put(entry.getKey(), new FullTableName(entry.getValue().getDatabase(), entry.getValue().getName()));
        }
        return deepCopy;
    }
}