package com.booking.replication.binlog.event;

import com.github.shyiko.mysql.binlog.event.Event;
import com.google.code.or.binlog.BinlogEventV4;

public interface IBinlogEvent {

    void setBinlogFilename(String binlogFilename);

    boolean hasHeader();

    long getTimestampOfReceipt();

    long getTimestamp();

    String getBinlogFilename();

    void overrideTimestamp(long newTimestampValue);

    boolean isQuery();

    String getQuerySQL();

    boolean isTableMap();

    boolean isUpdateRows();

    boolean isWriteRows();

    boolean isDeleteRows();

    boolean  isXid();

    boolean isFormatDescription();

    boolean isRotate();

    boolean isStop();

    BinlogEventType getEventType();

    String getFilename();

    long getPosition();

    long getNextPosition();

    BinlogEventV4 getBinlogEventV4();

    Event getBinlogConnectorEvent();
}
