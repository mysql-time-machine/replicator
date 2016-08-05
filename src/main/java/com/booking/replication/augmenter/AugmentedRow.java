package com.booking.replication.augmenter;

import com.booking.replication.schema.column.ColumnSchema;
import com.booking.replication.schema.exception.TableMapException;
import com.booking.replication.schema.table.TableSchema;
import com.booking.replication.util.JsonBuilder;

import com.google.code.or.binlog.BinlogEventV4Header;
import com.google.code.or.binlog.impl.event.BinlogEventV4HeaderImpl;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

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
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties({"tableSchema"})
public class AugmentedRow {

    @JsonDeserialize(as = BinlogEventV4HeaderImpl.class)
    @JsonIgnoreProperties({"headerLength", "position"})
    private BinlogEventV4Header eventV4Header;

    @JsonDeserialize(as = TableSchema.class)
    private TableSchema tableSchema;

    private final String       binlogFileName;
    private final long         rowBinlogEventOrdinal;
    private final String       tableName;
    private final List<String> primaryKeyColumns = new ArrayList<>();

    private final String       rowUUID;
    private final String       rowBinlogPositionID;

    // eventColumns: {
    //          column_name  => $name,
    //          value_before => $v1,
    //          value_after  => $v2,
    //          type         => $type
    //      }
    // }
    private HashMap<String,Map<String,String>> eventColumns = new HashMap<>();

    private String eventType;

    /**
     * Create AugmentedRow.
     *
     * @param binlogFileName      Name of the binlog file that contains current row
     * @param rowOrdinal          Order of the row in the binlog event that contains the row
     * @param tableName           Table name of the row
     * @param tableSchema         TableSchema object
     * @param eventType           Event type identifier (INSERT/UPDATE/DELETE)
     * @param binlogEventV4Header BinlogEventV4Header object
     *
     * @throws InvalidParameterException    Invalid parameter
     * @throws TableMapException            Invalid table
     */
    public AugmentedRow(
            String              binlogFileName,
            long                rowOrdinal,
            String              tableName,
            TableSchema         tableSchema,
            String              eventType,
            BinlogEventV4Header binlogEventV4Header
    ) {
        this.rowBinlogEventOrdinal = rowOrdinal;
        this.binlogFileName = binlogFileName;
        this.tableName = tableName;
        this.tableSchema = tableSchema;
        this.eventType = eventType;
        this.eventV4Header = binlogEventV4Header;

        Long eventPosition = eventV4Header.getPosition();

        rowBinlogPositionID = String.format("%s:%020d:%03d", this.binlogFileName, eventPosition, this.rowBinlogEventOrdinal);
        rowUUID = UUID.randomUUID().toString();;
    }

    /**
     * Add column data.
     *
     * @param columnName    Name of the column to update
     * @param valueBefore   Value before the update
     * @param valueAfter    Value after the update
     * @param columnType    Column type
     * @throws InvalidParameterException    Invalid parameter
     * @throws TableMapException            Invalid table
     */
    public void addColumnDataForUpdate(
            String columnName,
            String valueBefore,
            String valueAfter,
            String columnType
    ) throws InvalidParameterException, TableMapException {

        if (columnName == null) {
            throw new InvalidParameterException("columnName can not be null");
        } else if (eventColumns.get(columnName) == null) {
            String errorMessage = "Missing data slot for { table => " + this.getTableName() + ", columnName => " + columnName;
            errorMessage += "\n Known columns for table " + this.getTableName() + " are:";
            for (String c : eventColumns.keySet()) {
                errorMessage += "\n\t" + c;
            }
            throw new TableMapException(errorMessage);
        } else {
            eventColumns.get(columnName).put("value_before", valueBefore);
            eventColumns.get(columnName).put("value_after", valueAfter);
            eventColumns.get(columnName).put("type", columnType);
        }
    }

    /**
     * Add column data.
     * @param columnName Name of the column to insert
     * @param value       Value to insert
     * @param columnType  Column type
     */
    public void addColumnDataForInsert(
            String columnName,
            String value,
            String columnType) {
        eventColumns.get(columnName).put("value", value);
        eventColumns.get(columnName).put("type", columnType);
    }

    /**
     * Set table schema.
     *
     * @param tableSchema           Schema of the table
     * @throws TableMapException    Invalid table
     */
    public void setTableSchema(TableSchema tableSchema) throws TableMapException {
        this.tableSchema = tableSchema;
        initPKList();
        initColumnDataSlots();
    }

    /**
     * Initialize column data slots.
     *
     * <p>Pre-create objects for speed (this improves overall runtime speed ~10%)</p>
     */
    public void initColumnDataSlots() {
        for (String columnName: tableSchema.getColumnIndexToNameMap().values()) {
            eventColumns.put(columnName, new HashMap<String, String>());
        }
    }

    public String toJson() {
        return JsonBuilder.augmentedRowToJson(this);
    }

    /**
     * Initialize primary key columns.
     *
     * @throws TableMapException    Invalid table.
     */
    public void initPKList() throws TableMapException {
        if (tableSchema == null) {
            throw new TableMapException("Need table schem in order to generate PK list.");
        } else {
            Map<Integer, String> pkColumns = new HashMap<>();

            Set<String> columnNames = tableSchema.getColumnsSchema().keySet();
            for (String columnName: columnNames) {

                ColumnSchema cs = tableSchema.getColumnsSchema().get(columnName);

                String  ck = cs.getColumnKey();
                Integer op = cs.getOrdinalPosition();
                String  cn = cs.getColumnName();

                if ((ck != null) && (ck.equals("PRI"))) {
                    pkColumns.put(op, cn);
                }
            }

            TreeMap<Integer,String> pkColumnSortedByOP = new TreeMap<>();
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

    public String getRowBinlogPositionID() {
        return this.rowBinlogPositionID;
    }

    public String getRowUUID() {
        return rowUUID;
    }

    public String getTableName() {
        return tableName;
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

    public String getBinlogFileName() {
        return binlogFileName;
    }

    public long getRowBinlogEventOrdinal() {
        return rowBinlogEventOrdinal;
    }
}
