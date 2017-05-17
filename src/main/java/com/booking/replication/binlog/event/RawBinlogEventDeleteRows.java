package com.booking.replication.binlog.event;

import com.booking.replication.binlog.common.Cell;
import com.booking.replication.binlog.common.CellExtractor;
import com.booking.replication.binlog.common.Row;
import com.github.shyiko.mysql.binlog.event.DeleteRowsEventData;
import com.github.shyiko.mysql.binlog.event.WriteRowsEventData;
import com.google.code.or.binlog.impl.event.DeleteRowsEvent;
import com.google.code.or.binlog.impl.event.DeleteRowsEventV2;
import com.google.code.or.binlog.impl.event.WriteRowsEvent;
import com.google.code.or.common.glossary.Column;
import com.google.code.or.common.util.MySQLConstants;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

/**
 * Created by bosko on 6/1/17.
 */
public class RawBinlogEventDeleteRows extends RawBinlogEventRows {

    List<Row> extractedRows;

    public RawBinlogEventDeleteRows(Object event) throws Exception {
        super(event);
        extractedRows = this.extractRowsFromEvent();
    }

    public int getColumnCount() {
        if (this.binlogEventV4 != null) {
            if (binlogEventV4.getHeader().getEventType() == MySQLConstants.DELETE_ROWS_EVENT) {
                return ((DeleteRowsEvent) binlogEventV4).getColumnCount().intValue();
            }
            else {
                return ((DeleteRowsEventV2) binlogEventV4).getColumnCount().intValue();
            }
        }
        else {
            BitSet includedColumns = ((DeleteRowsEventData) binlogConnectorEvent.getData()).getIncludedColumns();
            return includedColumns.cardinality();
        }
    }

    public long getTableId() {
        if (this.binlogEventV4 != null) {
            if (binlogEventV4.getHeader().getEventType() == MySQLConstants.DELETE_ROWS_EVENT) {
                return ((DeleteRowsEvent) binlogEventV4).getTableId();
            }
            else {
                return ((DeleteRowsEventV2) binlogEventV4).getTableId();
            }
        }
        else {
            return ((DeleteRowsEventData) binlogConnectorEvent.getData()).getTableId();
        }
    }

    public List<Row> getExtractedRows() {
        return extractedRows;
    }

    private List<Row> extractRowsFromEvent() throws Exception {

        List<Row> rows = new ArrayList();

        if (this.USING_DEPRECATED_PARSER) {
            if (binlogEventV4.getHeader().getEventType() == MySQLConstants.DELETE_ROWS_EVENT) {
                for (com.google.code.or.common.glossary.Row orRow : ((DeleteRowsEvent) binlogEventV4).getRows()) {
                    List<Cell> cells = new ArrayList<>();
                    for (Column column: orRow.getColumns()) {
                        Cell cell = CellExtractor.extractCellFromOpenReplicatorColumn(column);
                        cells.add(cell);
                    }
                    Row row = new Row(cells);
                    rows.add(row);
                }
            }
            else {
                for (com.google.code.or.common.glossary.Row orRow : ((DeleteRowsEventV2) binlogEventV4).getRows()) {
                    List<Cell> cells = new ArrayList<>();
                    for (Column column: orRow.getColumns()) {
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
            for (Serializable[] bcRow: ((DeleteRowsEventData) binlogConnectorEvent.getData()).getRows()) {
                List<Cell> cells = new ArrayList<>();
                for (int columnIndex = 0; columnIndex < bcRow.length; columnIndex++) {
                    Cell cell = CellExtractor.extractCellFromBinlogConnectorColumn(bcRow[columnIndex]);
                    cells.add(cell);
                }
                Row row = new Row(cells);
                rows.add(row);
            }
            return rows;
        }
    }
}
