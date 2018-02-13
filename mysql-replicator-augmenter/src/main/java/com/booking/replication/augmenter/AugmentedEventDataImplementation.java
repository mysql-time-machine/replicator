package com.booking.replication.augmenter;

import com.booking.replication.model.augmented.AugmentedEventData;
import com.booking.replication.model.augmented.AugmentedRow;
import com.booking.replication.model.augmented.active.schema.TableSchemaVersion;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class AugmentedEventDataImplementation implements AugmentedEventData {

    private String mysqlTableName;
    private String binlogFileName;
    private List<AugmentedRow> singleRowEvents = new ArrayList<>();

    AugmentedEventDataImplementation(String binlogFileName) {
        this.binlogFileName = binlogFileName;
    }

    public void addSingleRowEvent(AugmentedRow au) {
        singleRowEvents.add(au);
    }

    public List<AugmentedRow> getSingleRowEvents() {
        return singleRowEvents;
    }

    public String getTableName() {
        return mysqlTableName;
    }

    public void setMysqlTableName(String mysqlTableName) {
        this.mysqlTableName = mysqlTableName;
    }

    public String getBinlogFileName() {
        return binlogFileName;
    }
}
