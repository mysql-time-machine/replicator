package com.booking.replication.schema.column.types;

import com.booking.replication.schema.column.ColumnSchema;
import com.google.common.base.Joiner;
import org.apache.commons.lang.StringUtils;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by bdevetak on 24/11/15.
 */
public class SetColumnSchema extends ColumnSchema {

    private String[] setMembers;

    public SetColumnSchema(ResultSet tableInfoResultSet) throws SQLException {
        super(tableInfoResultSet);
        extractEnumValues(this.getCOLUMN_TYPE());
    }

    private void extractEnumValues(String mysqlSetInfo) {

        String setPattern = "(?<=set\\()(.*?)(?=\\))";

        Pattern p = Pattern.compile(setPattern, Pattern.CASE_INSENSITIVE);

        Matcher m = p.matcher(mysqlSetInfo);

        m.find();

        String setCSV = m.group(1);

        setMembers = StringUtils.split(setCSV,",");
        for (int i = 0; i < setMembers.length; i++) {
            setMembers[i] = setMembers[i].replace("'","");
        }
    }

    public String getSetMembersFromNumericValue(long setNumericValue) {
        // TODO: handle case when enum value is NULL
        List<String> items = new ArrayList<>();
        if (setNumericValue == 0) {
            return "";
        }
        else {
            for (int i = 0; i < setMembers.length; i++) {
                if (((setNumericValue >> i) & 1) == 1) {
                    items.add(setMembers[i]);
                }
            }
            return Joiner.on(",").join(items);
        }
    }
}
