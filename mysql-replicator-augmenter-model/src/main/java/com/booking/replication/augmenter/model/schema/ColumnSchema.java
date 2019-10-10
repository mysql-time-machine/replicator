package com.booking.replication.augmenter.model.schema;

import com.fasterxml.jackson.annotation.JsonFilter;
import com.fasterxml.jackson.annotation.JsonIgnore;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.Serializable;

@SuppressWarnings("unused")
@JsonFilter("column")
public class ColumnSchema implements Cloneable, Serializable {
    private static final Logger LOG = LogManager.getLogger(ColumnSchema.class);

    private String name;
    private String columnType;
    private String key;
    private String valueDefault;
    private String collation;

    private DataType dataType;

    private Integer charMaxLength;

    private boolean isNullable;

    @JsonIgnore
    public boolean primary; // temp transition

    public ColumnSchema() { }

    public ColumnSchema(
            String columnName,
            DataType dataType,
            String columnType,
            boolean isNullable,
            String key
    ) {
        this.name       = columnName;
        this.dataType   = dataType;
        this.columnType = columnType;
        this.isNullable = isNullable;
        this.key        = key;
    }

    public ColumnSchema setDefaultValue(String defaultValue) {
        this.valueDefault = defaultValue;

        return this;
    }

    public ColumnSchema setCollation(String collation) {
        this.collation  = collation;

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

    public String getCollation() {
        return collation;
    }

    public boolean isNullable() {
        return this.isNullable;
    }

    public String getKey() {
        return this.key;
    }

    public String getValueDefault() {
        return this.valueDefault;
    }

    public Integer getCharMaxLength() {
        return charMaxLength;
    }

    public boolean isPrimary() {
        return key.equalsIgnoreCase("PRI");
    }

    public ColumnSchema deepCopy() {
        try {
            return (ColumnSchema) this.clone();
        } catch (CloneNotSupportedException ex) {
            LOG.warn("Not able to clone ColumnSchema", ex);

            ColumnSchema schema = new ColumnSchema(
                    this.name,
                    this.dataType,
                    this.columnType,
                    this.isNullable,
                    this.key
            );

            schema.setDefaultValue(this.valueDefault)
                    .setCharMaxLength(this.charMaxLength)
                    .setCollation(this.collation);

            return schema;
        }
    }
}