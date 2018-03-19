package com.booking.replication.model.row;

import com.booking.replication.model.cell.Cell;
import com.google.common.base.Joiner;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by bosko on 6/21/17.
 * <p>
 * ParsedRow is just a list of ParsedColumns.
 */
public class Row {

    private List<Cell> rowCells;

    /**
     *
     */
    public Row() {
    }

    public Row(List<Cell> rowCells) {
        this.rowCells = rowCells;
    }

    /**
     *
     */
    @Override
    public String toString() {
        List<String> cl = rowCells.stream()
                .map(c -> c.getValue().toString())
                .collect(Collectors.toList());
        return "rowCells: " + Joiner.on(",").join(cl);
    }

    /**
     *
     */
    public List<Cell> getRowCells() {
        return rowCells;
    }
}
