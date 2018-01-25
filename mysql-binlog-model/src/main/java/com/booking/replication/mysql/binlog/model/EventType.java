package com.booking.replication.mysql.binlog.model;

@SuppressWarnings("unused")
public enum EventType {
    UNKNOWN(0),
    START_V3(1),
    QUERY(2),
    STOP(3),
    ROTATE(4),
    INTVAR(5),
    LOAD(6),
    SLAVE(7),
    CREATE_FILE(8),
    APPEND_BLOCK(9),
    EXEC_LOAD(10),
    DELETE_FILE(11),
    NEW_LOAD(12),
    RAND(13),
    USER_VAR(14),
    FORMAT_DESCRIPTION(15),
    XID(16),
    BEGIN_LOAD_QUERY(17),
    EXECUTE_LOAD_QUERY(18),
    TABLE_MAP(19),
    PRE_GA_WRITE_ROWS(20),
    PRE_GA_UPDATE_ROWS(21),
    PRE_GA_DELETE_ROWS(22),
    WRITE_ROWS(23),
    UPDATE_ROWS(24),
    DELETE_ROWS(25),
    INCIDENT(26),
    HEARTBEAT(27),
    IGNORABLE(28),
    ROWS_QUERY(29),
    EXT_WRITE_ROWS(30),
    EXT_UPDATE_ROWS(31),
    EXT_DELETE_ROWS(32),
    GTID(33),
    ANONYMOUS_GTID(34),
    PREVIOUS_GTIDS(35),
    TRANSACTION_CONTEXT(36),
    VIEW_CHANGE(37),
    XA_PREPARE(38),
    AUGMENTED(99);

    private int code;

    EventType(int code) {
        this.code = code;
    }
}
