package com.booking.replication.binlog.event;

/**
 * Created by bosko on 5/17/17.
 */
public enum BinlogEventType {

    QUERY_EVENT,

    TABLE_MAP_EVENT,

    UPDATE_ROWS_EVENT,

    WRITE_ROWS_EVENT,

    DELETE_ROWS_EVENT,

    XID_EVENT,

    FORMAT_DESCRIPTION_EVENT,

    ROTATE_EVENT,

    STOP_EVENT,

    UNKNOWN

}
