package com.booking.replication.binlog.event.impl;

import com.booking.replication.binlog.common.Cell;
import com.booking.replication.binlog.common.CellExtractor;
import com.booking.replication.binlog.common.Row;
import com.github.shyiko.mysql.binlog.event.WriteRowsEventData;
import com.google.code.or.binlog.impl.event.WriteRowsEvent;
import com.google.code.or.binlog.impl.event.WriteRowsEventV2;
import com.google.code.or.common.glossary.Column;
import com.google.code.or.common.util.MySQLConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;

/**
 * Created by bosko on 6/1/17.
 */
public class BinlogEventWriteRows extends BinlogEventRows {

    private static final Logger LOGGER = LoggerFactory.getLogger(BinlogEventWriteRows.class);

    List<Row> extractedRows;

    public BinlogEventWriteRows(Object event) throws Exception {
        super(event);
        extractedRows = this.extractRowsFromEvent();
    }

    public int getColumnCount() {
        if (this.USING_DEPRECATED_PARSER) {
            if (binlogEventV4.getHeader().getEventType() == MySQLConstants.WRITE_ROWS_EVENT) {
                return ((WriteRowsEvent) binlogEventV4).getColumnCount().intValue();
            } else {
                return ((WriteRowsEventV2) binlogEventV4).getColumnCount().intValue();
            }
        }
        else {
            BitSet includedColumns = ((WriteRowsEventData) binlogConnectorEvent.getData()).getIncludedColumns();
            return includedColumns.cardinality();
        }
    }

    public long getTableId() {
        if (this.USING_DEPRECATED_PARSER) {
            if (binlogEventV4.getHeader().getEventType() == MySQLConstants.WRITE_ROWS_EVENT) {
                return ((WriteRowsEvent) binlogEventV4).getTableId();
            }
            else {
                return ((WriteRowsEventV2) binlogEventV4).getTableId();
            }
        }
        else {
            return ((WriteRowsEventData) binlogConnectorEvent.getData()).getTableId();
        }
    }

    public List<Row> getExtractedRows() {
        return extractedRows;
    }

    private List<Row> extractRowsFromEvent() throws Exception {

        List<Row> rows = new ArrayList();

        if (this.USING_DEPRECATED_PARSER) {
            if (binlogEventV4.getHeader().getEventType() == MySQLConstants.WRITE_ROWS_EVENT) {
                for (com.google.code.or.common.glossary.Row orRow : ((WriteRowsEvent) binlogEventV4).getRows()) {
                    List<Cell> cells = new ArrayList<>();
                    for (Column column : orRow.getColumns()) {
                        Cell cell = CellExtractor.extractCellFromOpenReplicatorColumn(column);
                        cells.add(cell);
                    }
                    Row row = new Row(cells);
                    rows.add(row);
                }
            }
            else {
                for (com.google.code.or.common.glossary.Row orRow : ((WriteRowsEventV2) binlogEventV4).getRows()) {
                    List<Cell> cells = new ArrayList<>();
                    for (Column column : orRow.getColumns()) {
                        Cell cell = CellExtractor.extractCellFromOpenReplicatorColumn(column);
                        cells.add(cell);
                    }
                    Row row = new Row(cells);
                    rows.add(row);
                }
            }
            return rows;
        }
        else {
            WriteRowsEventData data = binlogConnectorEvent.getData();

            for (Serializable[] bcRow : data.getRows()) {
                List<Cell> cells = new ArrayList<>();

                for (Serializable column: bcRow) {
                    if (column == null) {
                    } else {
                        Cell cell = CellExtractor.extractCellFromBinlogConnectorColumn(column);
                        cells.add(cell);
                    }
                }
                rows.add(new Row(cells));
            }

            return rows;
        }
    }
}
