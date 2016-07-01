package com.booking.replication.augmenter;

import com.google.code.or.binlog.BinlogEventV4;
import com.google.code.or.binlog.impl.event.AbstractRowEvent;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by bdevetak on 20/11/15.
 */
public class AugmentedRowsEvent {

    AugmentedRowsEvent(AbstractRowEvent ev) {
        binlogFileName = ev.getBinlogFilename();
    }

    private String mysqlTableName;

    private List<AugmentedRow> singleRowEvents = new ArrayList<>();

    private String binlogFileName;

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

    public void setBinlogFileName(String binlogFileName) {
        this.binlogFileName = binlogFileName;
    }
}
