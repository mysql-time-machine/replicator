package com.booking.replication.mysql.binlog.model;

import java.io.Serializable;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Map;

public interface AugmentedEvent extends Serializable, Event {
    public void addColumnDataToEvent();
    public void initColumnDataSlots();
    public void initPKList();
    public String getTableName();

    public Map<String, Map<String, String>> getEventColumns();

    public void setEventColumns(Map<String, Map<String, String>> eventColumns);

    public TableSchemaVersion getTableSchemaVersion();

    public void setPrimaryKeyColumns(List<String> primaryKeyColumns);

    public List<String> getPrimaryKeyColumns();

}
