package com.booking.replication.augmenter;

import com.booking.replication.binlog.event.RawBinlogEventRows;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by bdevetak on 20/11/15.
 */
public class AugmentedRowsEvent {

    private String mysqlTableName;
    private String binlogFileName;
    private List<AugmentedRow> singleRowEvents = new ArrayList<>();

    AugmentedRowsEvent(RawBinlogEventRows ev) {
        binlogFileName = ev.getBinlogFilename();
    }

    public void addSingleRowEvent(AugmentedRow au) {
        singleRowEvents.add(au);
    }

    public List<AugmentedRow> getSingleRowEvents() {
        return singleRowEvents;
    }

    public String getMysqlTableName() {
        return mysqlTableName;
    }

    public void setMysqlTableName(String mysqlTableName) {
        this.mysqlTableName = mysqlTableName;
    }

    public String getBinlogFileName() {
        return binlogFileName;
    }

}
