package com.booking.replication.augmenter.model.schema;


import java.util.HashMap;
import java.util.Map;

public enum DataType {

    TINYINT("TINYINT"),
    SMALLINT("SMALLINT"),
    MEDIUMINT("MEDIUMINT"),
    INT("INT"),
    BIGINT("BIGINT"),

    DECIMAL("DECIMAL"),
    NEWDECIMAL("NEWDECIMAL"),

    FLOAT("FLOAT"),
    DOUBLE("DOUBLE"),

    BINARY("BINARY"),
    VARBINARY("VARBINARY"),

    CHAR("CHAR"),
    VARCHAR("VARCHAR"),

    TINYTEXT("TINYTEXT"),
    TEXT("TEXT"),
    MEDIUMTEXT("MEDIUMTEXT"),
    LONGTEXT("LONGTEXT"),

    TINYBLOB("TINYBLOB"),
    MEDIUMBLOB("MEDIUMBLOB"),
    BLOB("BLOB"),
    LONGBLOB("LONGBLOB"),

    BIT("BIT"),

    ENUM("ENUM"),
    SET("SET"),

    DATE("DATE"),
    TIME("TIME"),
    DATETIME("DATETIME"),
    TIMESTAMP("TIMESTAMP"),
    YEAR("YEAR"),
    NEWDATE("NEWDATE"),
    TIMESTAMP_V2("TIMESTAMP_V2"),
    DATETIME_V2("DATETIME_V2"),
    TIME_V2("TIME_V2"),

    JSON("JSON"),

    GEOMETRY("GEOMETRY"),
    POINT("POINT"),
    LINESTRING("LINESTRING"),
    POLYGON("POLYGON"),
    GEOMETRYCOLLECTION("GEOMETRYCOLLECTION"),
    MULTILINESTRING("MULTILINESTRING"),
    MULTIPOINT("MULTIPOINT"),
    MULTIPOLYGON("MULTIPOLYGON"),

    UNKNOWN("UNKNOWN");

    private String code;

    DataType(String code) {
        this.code = code;
    }

    public String getCode() {
        return code;
    }

    private static final Map<String, DataType> INDEX_BY_CODE;

    static {
        INDEX_BY_CODE = new HashMap<String, DataType>();
        for (DataType dataType : values()) {
            INDEX_BY_CODE.put(dataType.code.toUpperCase(), dataType);
        }
    }

    public static DataType byCode(String code) {
        return INDEX_BY_CODE.getOrDefault(code.toUpperCase(), DataType.UNKNOWN);
    }
}
