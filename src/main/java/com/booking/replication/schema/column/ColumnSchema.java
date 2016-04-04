package com.booking.replication.schema.column;

import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Created by bosko on 11/6/15.
 */
public class ColumnSchema {

    private String  COLUMN_NAME;
    private String  COLUMN_KEY;
    private String  CHARACTER_SET_NAME;
    private String  DATA_TYPE;
    private String  COLUMN_TYPE;
    private int     ORDINAL_POSITION; // ColumnSchema position in the table
    private int     CHARACTER_MAXIMUM_LENGTH;
    private boolean IS_NULLABLE;

    public ColumnSchema() {

    }

    public ColumnSchema(ResultSet tableInfoResultSet) throws SQLException {

        this.setCOLUMN_NAME(tableInfoResultSet.getString("COLUMN_NAME"));
        this.setCOLUMN_KEY(tableInfoResultSet.getString("COLUMN_KEY"));
        this.setDATA_TYPE(tableInfoResultSet.getString("DATA_TYPE"));
        this.setCOLUMN_TYPE(tableInfoResultSet.getString("COLUMN_TYPE"));
        this.setIS_NULLABLE(tableInfoResultSet.getBoolean("IS_NULLABLE"));
        this.setORDINAL_POSITION(tableInfoResultSet.getInt("ORDINAL_POSITION"));
        this.setCHARACTER_SET_NAME(tableInfoResultSet.getString("CHARACTER_SET_NAME"));
        this.setCHARACTER_MAXIMUM_LENGTH(tableInfoResultSet.getInt("CHARACTER_MAXIMUM_LENGTH"));

    }

    public String getCOLUMN_KEY() {
        return COLUMN_KEY;
    }

    public void setCOLUMN_KEY(String COLUMN_KEY) {
        this.COLUMN_KEY = COLUMN_KEY;
    }

    public String getCHARACTER_SET_NAME() {
        return CHARACTER_SET_NAME;
    }

    public void setCHARACTER_SET_NAME(String CHARACTER_SET_NAME) {
        this.CHARACTER_SET_NAME = CHARACTER_SET_NAME;
    }

    public String getDATA_TYPE() {
        return DATA_TYPE;
    }

    public void setDATA_TYPE(String DATA_TYPE) {
        this.DATA_TYPE = DATA_TYPE;
    }

    public int getCHARACTER_MAXIMUM_LENGTH() {
        return CHARACTER_MAXIMUM_LENGTH;
    }

    public void setCHARACTER_MAXIMUM_LENGTH(int CHARACTER_MAXIMUM_LENGTH) {
        this.CHARACTER_MAXIMUM_LENGTH = CHARACTER_MAXIMUM_LENGTH;
    }

    public boolean IS_NULLABLE() {
        return IS_NULLABLE;
    }

    public void setIS_NULLABLE(boolean IS_NULLABLE) {
        this.IS_NULLABLE = IS_NULLABLE;
    }

    public String getCOLUMN_NAME() {
        return COLUMN_NAME;
    }

    public void setCOLUMN_NAME(String COLUMN_NAME) {
        this.COLUMN_NAME = COLUMN_NAME;
    }

    public int getORDINAL_POSITION() {
        return ORDINAL_POSITION;
    }

    public void setORDINAL_POSITION(int ORDINAL_POSITION) {
        this.ORDINAL_POSITION = ORDINAL_POSITION;
    }

    public String getCOLUMN_TYPE() {
        return COLUMN_TYPE;
    }

    public void setCOLUMN_TYPE(String COLUMN_TYPE) {
        this.COLUMN_TYPE = COLUMN_TYPE;
    }
}
