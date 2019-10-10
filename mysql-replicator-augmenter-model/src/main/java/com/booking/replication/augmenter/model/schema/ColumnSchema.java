package com.booking.replication.augmenter.model.schema;

import com.fasterxml.jackson.annotation.JsonFilter;
import com.fasterxml.jackson.annotation.JsonIgnore;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.Serializable;
import java.util.List;
import java.util.Optional;

@SuppressWarnings("unused")
@JsonFilter("column")
public class ColumnSchema implements Cloneable, Serializable {

    private static final Logger LOG = LogManager.getLogger(ColumnSchema.class);

    private String name;
    private String columnType;
    private String valueDefault;
    private String collation;

    private DataType dataType;

    private Integer charMaxLength;

    private boolean isNullable;

    private Optional<List<String>> enumOrSetValueList;

    @JsonIgnore
    private boolean primary;

    public ColumnSchema() { }

    public ColumnSchema(
            String columnName,
            DataType dataType,
            String columnType,
            boolean isNullable,
            boolean isPrimary,
            Optional<List<String>> enumOrSetValueList
    ) {
        this.name       = columnName;
        this.dataType   = dataType;
        this.columnType = columnType;
        this.isNullable = isNullable;
        this.primary    = isPrimary;

        if (enumOrSetValueList.isPresent()) {
            this.enumOrSetValueList = enumOrSetValueList;

        } else {
            this.enumOrSetValueList = Optional.empty();
        }

    }

    public ColumnSchema setDefaultValue(String defaultValue) {
        this.valueDefault = defaultValue;
        return this;
    }

    public ColumnSchema setCollation(String collation) {
        this.collation = collation;
        return this;
    }

    public ColumnSchema setCharMaxLength(Integer charMaxLength) {
        this.charMaxLength = charMaxLength;
        return this;
    }

    public String getName() {
        return this.name;
    }

    public DataType getDataType() {
        return this.dataType;
    }

    public String getColumnType() {
        return this.columnType;
    }

    public void setColumnType(String columnType) {
        this.columnType = columnType;
    }

    public String getCollation() {
        return collation;
    }

    public boolean isNullable() {
        return this.isNullable;
    }

    public String getValueDefault() {
        return this.valueDefault;
    }

    public Integer getCharMaxLength() {
        return charMaxLength;
    }

    public boolean isPrimary() {
        return primary;
    }

    public Optional<List<String>> getEnumOrSetValueList() {
        return enumOrSetValueList;
    }

    public void setEnumOrSetValueList(Optional<List<String>> enumOrSetValueList) {
        this.enumOrSetValueList = enumOrSetValueList;
    }

    public ColumnSchema deepCopy() {

        ColumnSchema schema = new ColumnSchema(
                this.name,
                this.dataType,
                this.columnType,
                this.isNullable,
                this.isPrimary(),
                this.enumOrSetValueList
        );

        schema.setDefaultValue(this.valueDefault)
                .setCharMaxLength(this.charMaxLength)
                .setCollation(this.collation);

        return schema;
    }

}