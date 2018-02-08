package com.booking.replication.model;

import com.booking.replication.model.augmented.AugmentedEventData;
import com.booking.replication.model.augmented.AugmentedEventDataImplementation;
import com.booking.replication.model.transaction.TransactionEventData;
import com.booking.replication.model.transaction.TransactionEventDataImplementation;

@SuppressWarnings("unused")
public enum EventType {
    UNKNOWN(0, ByteArrayEventData.class),
    START_V3(1),
    QUERY(2, QueryEventData.class),
    STOP(3),
    ROTATE(4, RotateEventData.class),
    INTVAR(5, IntVarEventData.class),
    LOAD(6),
    SLAVE(7),
    CREATE_FILE(8),
    APPEND_BLOCK(9),
    EXEC_LOAD(10),
    DELETE_FILE(11),
    NEW_LOAD(12),
    RAND(13),
    USER_VAR(14),
    FORMAT_DESCRIPTION(15, FormatDescriptionEventData.class),
    XID(16, XIDEventData.class),
    BEGIN_LOAD_QUERY(17),
    EXECUTE_LOAD_QUERY(18),
    TABLE_MAP(19, TableMapEventData.class),
    PRE_GA_WRITE_ROWS(20),
    PRE_GA_UPDATE_ROWS(21),
    PRE_GA_DELETE_ROWS(22),
    WRITE_ROWS(23, WriteRowsEventData.class),
    UPDATE_ROWS(24, UpdateRowsEventData.class),
    DELETE_ROWS(25, DeleteRowsEventData.class),
    INCIDENT(26),
    HEARTBEAT(27),
    IGNORABLE(28),
    ROWS_QUERY(29, RowsQueryEventData.class),
    EXT_WRITE_ROWS(30),
    EXT_UPDATE_ROWS(31),
    EXT_DELETE_ROWS(32),
    GTID(33, GTIDEventData.class),
    ANONYMOUS_GTID(34),
    PREVIOUS_GTIDS(35, PreviousGTIDSetEventData.class),
    TRANSACTION_CONTEXT(36),
    VIEW_CHANGE(37),
    XA_PREPARE(38, XAPrepareEventData.class),
    TRANSACTION(100, TransactionEventData.class, TransactionEventDataImplementation.class),
    AUGMENTED_INSERT(101, AugmentedEventData.class, AugmentedEventDataImplementation.class),
    AUGMENTED_UPDATE(102, AugmentedEventData.class, AugmentedEventDataImplementation.class),
    AUGMENTED_DELETE(103, AugmentedEventData.class, AugmentedEventDataImplementation.class),
    AUGMENTED_SCHEMA(104);

    private final int code;
    private final Class<? extends EventData> definition;
    private final Class<? extends EventData> implementation;

    EventType(int code, Class<? extends EventData> definition, Class<? extends EventData> implementation) {
        this.code = code;
        this.definition = definition;
        this.implementation = implementation;
    }

    EventType(int code, Class<? extends EventData> definition) {
        this(code, definition, null);
    }

    EventType(int code) {
        this(code, EventData.class);
    }

    public int getCode() {
        return this.code;
    }

    public Class<? extends EventData> getDefinition() {
        return this.definition;
    }

    public Class<? extends EventData> getImplementation() {
        return this.implementation;
    }
}
