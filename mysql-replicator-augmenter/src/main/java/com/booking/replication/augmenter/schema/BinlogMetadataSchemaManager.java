package com.booking.replication.augmenter.schema;

import com.booking.replication.augmenter.SchemaManager;
import com.booking.replication.augmenter.model.schema.ColumnSchema;
import com.booking.replication.augmenter.model.schema.SchemaAtPositionCache;
import com.booking.replication.augmenter.model.schema.TableSchema;
import com.booking.replication.supplier.model.TableMapRawEventData;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class BinlogMetadataSchemaManager implements SchemaManager {

    private static final Logger LOG = LogManager.getLogger(BinlogMetadataSchemaManager.class);

    private final Map<String, TableMapRawEventData> tableMapEventDataCache;
    private final SchemaAtPositionCache schemaAtPositionCache;
    private final Function<String, TableSchema> schemaComputeFn;

    public BinlogMetadataSchemaManager(Map<String, Object> configuration) {

        this.tableMapEventDataCache = new HashMap<>();
        this.schemaAtPositionCache = new SchemaAtPositionCache();

        this.schemaComputeFn = (tableName) -> {
            try {
                TableMapRawEventData tableMapRawEventData = this.tableMapEventDataCache.get(tableName);
                Object schema = tableMapRawEventData.getDatabase();
                TableSchema ts = SchemaUtil.computeTableSchemaFromBinlogMetadata(
                        schema.toString(),
                        tableName,
                        tableMapRawEventData
                );
                ts.getColumnSchemas().stream().forEach(cs -> {
                        String message = "computed schema for { columnName: " + cs.getName() +
                                ", columnType: " + cs.getColumnType() +
                                ", collation: " + cs.getCollation() + " }";
                        System.out.println(message);
                });
                return ts;
            } catch (Exception e) {
                LOG.info("ERROR: Could not compute table schema for table: " + tableName);
                e.printStackTrace();
                return null;
            }
        };
    }

    @Override
    public Function<String, TableSchema> getComputeTableSchemaLambda() {
        return schemaComputeFn;
    }

    @Override
    public void updateTableMapCache(TableMapRawEventData tableMapRawEventData) {
        this.tableMapEventDataCache.put(tableMapRawEventData.getTable(), tableMapRawEventData);
    }

    @Override
    public List<ColumnSchema> listColumns(String tableName) {
        TableSchema tableSchema =
                this.schemaAtPositionCache.getTableColumns(tableName, this.schemaComputeFn);
        if (tableSchema == null) {
            return null;
        }
        return (List<ColumnSchema>) tableSchema.getColumnSchemas();
    }

    @Override
    public boolean execute(String tableName, String query) {
        // TODO: move this method to separate interface since its not needed in BinlogMetadataSchemaManager
        return true;
    }


    @Override
    public boolean dropTable(String tableName) throws SQLException {
        // TODO: move this method to separate interface since its not needed in BinlogMetadataSchemaManager
        return true;
    }

    @Override
    public String getCreateTable(String tableName) {
        // TODO: move this method to separate interface since its not needed in BinlogMetadataSchemaManager
        return "NA";
    }

    @Override
    public void close() throws IOException {
        // TODO: move this method to separate interface since its not needed in BinlogMetadataSchemaManager
    }

}
