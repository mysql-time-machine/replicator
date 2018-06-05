package com.booking.replication.augmenter;

import com.booking.replication.binlog.event.impl.BinlogEventRows;
import com.google.code.or.binlog.impl.event.AbstractRowEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by bdevetak on 20/11/15.
 */
public class AugmentedRowsEvent {

    private static final Logger LOGGER = LoggerFactory.getLogger(AugmentedRowsEvent.class);

    AugmentedRowsEvent(AbstractRowEvent ev) {
        binlogFileName = ev.getBinlogFilename();
    }

    private String mysqlTableName;
    private String binlogFileName;
    private List<AugmentedRow> singleRowEvents = new ArrayList<>();

    AugmentedRowsEvent(BinlogEventRows ev) {
        binlogFileName = ev.getBinlogFilename();
    }

    public void addSingleRowEvent(AugmentedRow au) {
        singleRowEvents.add(au);
    }

    public AugmentedRowsEvent removeRowsWithoutPrimaryKey() {

        List<AugmentedRow> singleRowEventsWithPK =
            singleRowEvents
                .stream()
                .filter(row -> !row.getPrimaryKeyColumns().isEmpty())
                .collect(Collectors.toList());

        if (singleRowEventsWithPK.size() != singleRowEvents.size()) {
            List<AugmentedRow> singleRowEventsWithoutPK =
                singleRowEvents
                    .stream()
                    .filter(row -> row.getPrimaryKeyColumns().isEmpty())
                    .collect(Collectors.toList());

            for (AugmentedRow row: singleRowEventsWithoutPK) {
                LOGGER.warn(
                    "Row without primary key detected. Tables/Rows without primary key are not replicated! Table name: "
                    + row.getTableName()
                );
            }
        }

        singleRowEvents = singleRowEventsWithPK;

        return this;
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
