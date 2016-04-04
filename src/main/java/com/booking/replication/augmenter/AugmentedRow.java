package com.booking.replication.augmenter;

import com.booking.replication.schema.table.TableSchema;
import com.booking.replication.schema.column.ColumnSchema;
import com.booking.replication.schema.exception.TableMapException;
import com.booking.replication.util.JSONBuilder;
import com.google.code.or.binlog.BinlogEventV4Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.InvalidParameterException;
import java.util.*;

/**
 *  Before actually applying event to HBase table,
 *  we need to know some data about the schema.
 *  From raw event (which is BinlogEventV4) and
 *  metaPosition (which is the corresponding schema
 *  version for that event) we do a matching of
 *  column names and types and construct augmented
 *  event which has both schema and data.
 *  This class encapsulates this type of event.
 */
public class AugmentedRow {

    private BinlogEventV4Header eventV4Header;

    private TableSchema tableSchema;
    private String tableName;
    private List<String> primaryKeyColumns = new ArrayList<String>();
    // eventColumns: {
    //      column_name => $name,
    //          value_before => $1,
    //          value_after => $v2,
    //          ...,
    //          type => $type
    //      }
    // }
    private HashMap<String,Map<String,String>> eventColumns = new HashMap<String, Map<String, String>>();

    private String eventType;

    private static final Logger LOGGER = LoggerFactory.getLogger(AugmentedRow.class);

    public void addColumnDataForUpdate(
            String columnName,
            String valueBefore,
            String valueAfter
    ) throws InvalidParameterException, TableMapException {

        if (columnName == null) {
            throw new InvalidParameterException("columnName can not be null");
        }
        else if (eventColumns.get(columnName) == null) {
            String errorMessage = "Missing data slot for { table => " + this.getTableName() + ", columnName => " + columnName;
            errorMessage += "\n Known columns for table " + this.getTableName() + " are:";
            for (String c : eventColumns.keySet()) {
                errorMessage += "\n\t" + c;
            }
            throw new TableMapException(errorMessage);
        }
        else {
            eventColumns.get(columnName).put("value_before", valueBefore);
            eventColumns.get(columnName).put("value_after", valueAfter);
        }
    }

    public void addColumnDataForInsert(
            String columnName,
            String value
    ) {
        eventColumns.get(columnName).put("value", value);
    }

    public void setTableSchema(TableSchema tableSchema) throws TableMapException {
        this.tableSchema = tableSchema;
        initPKList();
        initColumnDataSlots();
    }

    // pre-create objects for speed (this improves overall runtime speed ~10%)
    public void initColumnDataSlots() {
        for (String columnName: this.tableSchema.getColumnIndexToNameMap().values()) {
            eventColumns.put(columnName, new HashMap<String, String>());
        }
    }

    public String toJSON() {
        String json = JSONBuilder.dataEventToJSON(this);
        return json;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public HashMap<String, Map<String, String>> getEventColumns() {
        return eventColumns;
    }

    public void setEventColumns(HashMap<String, Map<String, String>> eventColumns) {
        this.eventColumns = eventColumns;
    }

    public TableSchema getTableSchema() {
        return tableSchema;
    }

    public List<String> getPrimaryKeyColumns() {
        return primaryKeyColumns;
    }

    public void setPrimaryKeyColumns(List<String> primaryKeyColumns) {
        this.primaryKeyColumns = primaryKeyColumns;
    }

    public void initPKList() throws TableMapException {

        if (tableSchema == null) {
            throw new TableMapException("Need table schem in order to generate PK list.");
        }
        else {
            Map<Integer, String> pkColumns = new HashMap<Integer, String>();

            Set<String> columnNames = tableSchema.getColumnsSchema().keySet();
            for(String columnName: columnNames) {

                ColumnSchema cs = tableSchema.getColumnsSchema().get(columnName);

                String  CK = cs.getCOLUMN_KEY();
                Integer OP = cs.getORDINAL_POSITION();
                String  CN = cs.getCOLUMN_NAME();

                if ((CK != null) && (CK.equals("PRI"))) {
                    pkColumns.put(OP, CN);
                }
            }

            TreeMap<Integer,String> pkColumnSortedByOP= new TreeMap<Integer,String>();
            pkColumnSortedByOP.putAll(pkColumns);

            primaryKeyColumns.addAll(pkColumnSortedByOP.values());
        }
    }

    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    public BinlogEventV4Header getEventV4Header() {
        return eventV4Header;
    }

    public void setEventV4Header(BinlogEventV4Header eventV4Header) {
        this.eventV4Header = eventV4Header;
    }
}
