package com.booking.replication.augmenter.model.schema;

import com.fasterxml.jackson.annotation.JsonIgnore;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.Serializable;

@SuppressWarnings("unused")
public class ColumnSchema implements Cloneable, Serializable {
    private static final Logger LOG = LogManager.getLogger(ColumnSchema.class);

    private String name;
    private String columnType;
    private String key;
    private String valueDefault;
    private String extra;
    private String collation;

    private DataType dataType;

    private Integer charMaxLength;
    private Integer charOctetLength;
    private Integer numericPrecision;
    private Integer numericScale;
    private Integer dateTimePrecision;

    private boolean isNullable;

    @JsonIgnore
    public boolean primary; // temp transition

    public ColumnSchema() { }

    public ColumnSchema(
            String columnName,
            DataType dataType,
            String columnType,
            boolean isNullable,
            String key,
            String extra
    ) {
        this.name       = columnName;
        this.dataType   = dataType;
        this.columnType = columnType;
        this.isNullable = isNullable;
        this.key        = key;
        this.extra      = extra;
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

    public ColumnSchema setCharOctetLength(Integer charOctetLength) {
        this.charOctetLength = charOctetLength;

        return this;
    }

    public ColumnSchema setNumericPrecision(Integer numericPrecision) {
        this.numericPrecision = numericPrecision;

        return this;
    }

    public ColumnSchema setNumericScale(Integer numericScale) {
        this.numericScale = numericScale;

        return this;
    }

    public ColumnSchema setDateTimePrecision(Integer dateTimePrecision) {
        this.dateTimePrecision = dateTimePrecision;

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

    public String getExtra() {
        return this.extra;
    }

    public Integer getCharMaxLength() {
        return charMaxLength;
    }

    public Integer getCharOctetLength() {
        return charOctetLength;
    }

    public Integer getNumericPrecision() {
        return numericPrecision;
    }

    public Integer getNumericScale() {
        return numericScale;
    }

    public Integer getDateTimePrecision() {
        return dateTimePrecision;
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
                    this.key,
                    this.extra
            );

            schema.setDefaultValue(this.valueDefault)
                    .setCharMaxLength(this.charMaxLength)
                    .setCharOctetLength(this.charOctetLength)
                    .setCollation(this.collation)
                    .setDateTimePrecision(this.dateTimePrecision)
                    .setNumericPrecision(this.numericPrecision)
                    .setNumericScale(this.numericScale);

            return schema;
        }
    }
}