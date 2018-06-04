package com.booking.replication.augmenter.model;

@SuppressWarnings("unused")
public enum QueryAugmentedEventDataType {
    BEGIN(0, null),
    COMMIT(1, null),
    DDL_DEFINER(2, null),
    DDL_TABLE(3, null),
    DDL_TEMPORARY_TABLE(4, null),
    DDL_VIEW(5, null),
    DDL_ANALYZE(6, null),
    PSEUDO_GTID(7, null),
    UNKNOWN(8, null);

    private final int code;
    private final Class<? extends AugmentedEventData> definition;

    QueryAugmentedEventDataType(int code, Class<? extends AugmentedEventData> definition) {
        this.code = code;
        this.definition = definition;
    }

    public int getCode() {
        return this.code;
    }

    public Class<? extends AugmentedEventData> getDefinition() {
        return this.definition;
    }
}
