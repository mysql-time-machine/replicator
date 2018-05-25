package com.booking.replication.augmenter.column.types;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.booking.replication.augmenter.column.ColumnSchema;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class EnumColumnSchema extends ColumnSchema {

    private String[] enumValues;

    private static final Logger LOGGER = LoggerFactory.getLogger(EnumColumnSchema.class);

    public EnumColumnSchema(ResultSet tableInfoResultSet) throws SQLException {
        super(tableInfoResultSet);
        extractEnumValues(this.getColumnType());
    }

    private void extractEnumValues(String mysqlEnumInfo) {

        String enumPattern = "(?<=enum\\()(.*?)(?=\\))\\)$";

        Matcher matcher = Pattern
                .compile(enumPattern, Pattern.CASE_INSENSITIVE)
                .matcher(mysqlEnumInfo);

        matcher.find();

        String enumCsv = matcher.group(1);

        enumValues = StringUtils.split(enumCsv,",");
        for (int i = 0; i < enumValues.length; i++) {
            enumValues[i] = enumValues[i].replace("'","");
        }
    }

    public String getEnumValueFromIndex(int index) {
        // TODO: handle case when enum value is NULL
        if (index == 0) {
            return "";
        } else {
            try {
                return enumValues[index - 1];
            } catch (IndexOutOfBoundsException e) {
                LOGGER.error("Index out of bound exception", e);
                LOGGER.error("received index => " + index);
                LOGGER.error("available => ");
                for (String val : enumValues) {
                    LOGGER.info(val);
                }
                throw new IndexOutOfBoundsException();
            }

        }
    }
}
