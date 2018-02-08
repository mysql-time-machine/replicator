package com.booking.replication.model.row;

/**
 * Created by bosko on 7/18/17.
 */
public class RowPair {

    private final Row before;

    private final Row after;

    public RowPair(Row before, Row after) {
        this.before = before;
        this.after = after;
    }

    public Row getBefore() {
        return before;
    }

    public Row getAfter() {
        return after;
    }
}
