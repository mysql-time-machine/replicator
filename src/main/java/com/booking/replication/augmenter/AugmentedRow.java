package com.booking.replication.augmenter;

import com.booking.replication.schema.column.ColumnSchema;
import com.booking.replication.schema.exception.TableMapException;
import com.booking.replication.schema.table.TableSchemaVersion;
import com.booking.replication.util.CaseInsensitiveMap;
import com.booking.replication.util.JsonBuilder;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.code.or.binlog.BinlogEventV4Header;
import com.google.code.or.binlog.impl.event.BinlogEventV4HeaderImpl;

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
@JsonIgnoreProperties({"tableSchemaVersion"})
public class AugmentedRow {

    @JsonDeserialize(as = BinlogEventV4HeaderImpl.class)
    @JsonIgnoreProperties({"headerLength", "position"})
    private BinlogEventV4Header eventV4Header;

    @JsonDeserialize(as = TableSchemaVersion.class)
    private TableSchemaVersion tableSchemaVersion;

    private String       binlogFileName;
    private long         rowBinlogEventOrdinal;
    private String       tableName;
    private List<String> primaryKeyColumns = new ArrayList<>();

    private String       rowUUID;
    private String       rowBinlogPositionID;
    private UUID         transactionUUID;
    private Long         transactionXid;
    private Long         transactionUUIDTimestamp;
    private boolean      applyUuid = false;
    private boolean      applyXid = false;

    // eventColumns: {
    //          column_name  => $name,
    //          value_before => $v1,
    //          value_after  => $v2,
    //          type         => $type
    //      }
    // }
    private Map<String,Map<String,String>> eventColumns = new CaseInsensitiveMap<>();

    private String eventType;

    public AugmentedRow() {

    }

    /**
     * Create AugmentedRow.
     *
     * @param binlogFileName      Name of the binlog file that contains current row
     * @param rowOrdinal          Order of the row in the binlog event that contains the row
     * @param tableName           Table name of the row
     * @param tableSchemaVersion  TableSchemaVersion object
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
            TableSchemaVersion tableSchemaVersion,
            String              eventType,
            BinlogEventV4Header binlogEventV4Header,
            UUID                transactionUUID,
            Long                transactionXid,
            boolean applyUuid,
            boolean applyXid)  throws TableMapException {

        this.rowBinlogEventOrdinal = rowOrdinal;
        this.binlogFileName = binlogFileName;
        this.tableName = tableName;
        this.eventType = eventType;
        this.eventV4Header = binlogEventV4Header;
        this.transactionUUID = transactionUUID;
        this.transactionXid = transactionXid;
        this.applyUuid = applyUuid;
        this.applyXid = applyXid;

        if (tableName != null && tableSchemaVersion != null) initTableSchema(tableSchemaVersion);

        Long eventPosition = eventV4Header.getPosition();

        rowBinlogPositionID = String.format("%s:%020d:%020d", this.binlogFileName, eventPosition, this.rowBinlogEventOrdinal);
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
     * @param tableSchemaVersion           Schema of the table
     * @throws TableMapException    Invalid table
     */
    private void initTableSchema(TableSchemaVersion tableSchemaVersion) throws TableMapException {
        this.tableSchemaVersion = tableSchemaVersion;
        initPKList();
        initColumnDataSlots();
    }

    /**
     * Initialize column data slots.
     *
     * <p>Pre-create objects for speed (this improves overall runtime speed ~10%)</p>
     */
    public void initColumnDataSlots() {
        for (String columnName: tableSchemaVersion.getColumnNames()) {
            eventColumns.put(columnName, new HashMap<String, String>());
        }
        if (applyUuid) {
            eventColumns.put(EventAugmenter.UUID_FIELD_NAME, new HashMap<String, String>());
        }
        if (applyXid) {
            eventColumns.put(EventAugmenter.XID_FIELD_NAME, new HashMap<String, String>());
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
        if (tableSchemaVersion == null) {
            throw new TableMapException("Need table schem in order to generate PK list.");
        } else {
            Map<Integer, String> pkColumns = new HashMap<>();

            Set<String> columnNames = tableSchemaVersion.getColumnNames();
            for (String columnName: columnNames) {

                ColumnSchema cs = tableSchemaVersion.getColumnSchemaByColumnName(columnName);

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

    public BinlogEventV4Header getEventV4Header() {
        return eventV4Header;
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

    public Map<String, Map<String, String>> getEventColumns() {
        return eventColumns;
    }

    public void setEventColumns(Map<String, Map<String, String>> eventColumns) {
        this.eventColumns = eventColumns;
    }

    public TableSchemaVersion getTableSchemaVersion() {
        return tableSchemaVersion;
    }

    public void setPrimaryKeyColumns(List<String> primaryKeyColumns) {
        this.primaryKeyColumns = primaryKeyColumns;
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

    public UUID getTransactionUUID() {
        return transactionUUID;
    }

    public Long getTransactionXid() {
        return transactionXid;
    }

    public void setTransactionUUIDTimestamp(long value) {
        transactionUUIDTimestamp = value;
    }
    public Long getTransactionUUIDTimestamp() {
        if (transactionUUIDTimestamp != null) {
            return transactionUUIDTimestamp;
        } else {
            return getEventV4Header().getTimestamp();
        }
    }
}
