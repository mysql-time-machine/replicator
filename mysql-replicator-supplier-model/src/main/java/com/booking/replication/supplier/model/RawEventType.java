package com.booking.replication.supplier.model;

@SuppressWarnings("unused")
public enum RawEventType {
    UNKNOWN(0, ByteArrayRawEventData.class),
    START_V3(1),
    QUERY(2, QueryRawEventData.class),
    STOP(3),
    ROTATE(4, RotateRawEventData.class),
    INTVAR(5, IntVarRawEventData.class),
    LOAD(6),
    SLAVE(7),
    CREATE_FILE(8),
    APPEND_BLOCK(9),
    EXEC_LOAD(10),
    DELETE_FILE(11),
    NEW_LOAD(12),
    RAND(13),
    USER_VAR(14),
    FORMAT_DESCRIPTION(15, FormatDescriptionRawEventData.class),
    XID(16, XIDRawEventData.class),
    BEGIN_LOAD_QUERY(17),
    EXECUTE_LOAD_QUERY(18),
    TABLE_MAP(19, TableMapRawEventData.class),
    PRE_GA_WRITE_ROWS(20),
    PRE_GA_UPDATE_ROWS(21),
    PRE_GA_DELETE_ROWS(22),
    WRITE_ROWS(23, WriteRowsRawEventData.class),
    UPDATE_ROWS(24, UpdateRowsRawEventData.class),
    DELETE_ROWS(25, DeleteRowsRawEventData.class),
    INCIDENT(26),
    HEARTBEAT(27),
    IGNORABLE(28),
    ROWS_QUERY(29, RowsQueryRawEventData.class),
    EXT_WRITE_ROWS(30),
    EXT_UPDATE_ROWS(31),
    EXT_DELETE_ROWS(32),
    GTID(33, GTIDRawEventData.class),
    ANONYMOUS_GTID(34),
    PREVIOUS_GTIDS(35, PreviousGTIDSetRawEventData.class),
    TRANSACTION_CONTEXT(36),
    VIEW_CHANGE(37),
    XA_PREPARE(38, XAPrepareRawEventData.class);

    private final int code;
    private final Class<? extends RawEventData> definition;

    RawEventType(int code, Class<? extends RawEventData> definition) {
        this.code = code;
        this.definition = definition;
    }

    RawEventType(int code) {
        this(code, RawEventData.class);
    }

    public int getCode() {
        return this.code;
    }

    public Class<? extends RawEventData> getDefinition() {
        return this.definition;
    }
}
