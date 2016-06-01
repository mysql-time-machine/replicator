package com.booking.replication.metrics;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by mdutikov on 5/30/2016.
 */
public class RowTotals {
    protected ICounter rowsForInsertProcessed;
    protected ICounter rowsForUpdateProcessed;
    protected ICounter rowsForDeleteProcessed;

    // TODO: what's the difference between two of the following:
    protected ICounter totalHbaseRowsAffected;
    protected ICounter hbaseRowsAffected;

    public RowTotals()
    {
        this(
                new Counter("TOTAL_HBASE_ROWS_AFFECTED"),
                new Counter("ROWS_FOR_DELETE_PROCESSED"),
                new Counter("ROWS_FOR_INSERT_PROCESSED"),
                new Counter("ROWS_FOR_UPDATE_PROCESSED"),
                new Counter("HBASE_ROWS_AFFECTED"));
    }

    public RowTotals(
                  ICounter totalHbaseRowsAffected,
                  ICounter rowsForDeleteProcessed,
                  ICounter rowsForInsertProcessed,
                  ICounter rowsForUpdateProcessed,
                  ICounter hbaseRowsAffected)
    {
        // TODO: arg checks

        this.totalHbaseRowsAffected = totalHbaseRowsAffected;
        this.rowsForDeleteProcessed = rowsForDeleteProcessed;
        this.rowsForInsertProcessed = rowsForInsertProcessed;
        this.rowsForUpdateProcessed = rowsForUpdateProcessed;
        this.hbaseRowsAffected = hbaseRowsAffected;
    }

    public ICounter getHbaseRowsAffected() {
        return hbaseRowsAffected;
    }

    public ICounter getRowsForInsertProcessed() {
        return rowsForInsertProcessed;
    }

    public ICounter getRowsForUpdateProcessed() {
        return rowsForUpdateProcessed;
    }

    public ICounter getRowsForDeleteProcessed() {
        return rowsForDeleteProcessed;
    }

    public ICounter getTotalHbaseRowsAffected() {
        return totalHbaseRowsAffected;
    }

    public INameValue[] getAllNamesAndValues()
    {
        ArrayList<INameValue> all = new ArrayList<>();

        Class<?> nameValueClass = INameValue.class;

        for (Field field : getAllFields(new ArrayList<Field>(), this.getClass())) {
            if (nameValueClass.isAssignableFrom(field.getType())) {
                try
                {
                    INameValue nv = (INameValue)field.get(this);

                    all.add(nv);
                }
                catch (IllegalAccessException e)
                {
                    e.printStackTrace();
                }
            }
        }

        INameValue[] arr = new INameValue[all.size()];
        arr = all.toArray(arr);

        return arr;
    }

    private static List<Field> getAllFields(List<Field> fields, Class<?> type) {
        fields.addAll(Arrays.asList(type.getDeclaredFields()));

        if (type.getSuperclass() != null) {
            fields = getAllFields(fields, type.getSuperclass());
        }

        return fields;
    }

    public RowTotals copy()
    {
        return new RowTotals(
                totalHbaseRowsAffected.copy(),
                rowsForDeleteProcessed.copy(),
                rowsForInsertProcessed.copy(),
                rowsForUpdateProcessed.copy(),
                getHbaseRowsAffected().copy());
    }
}