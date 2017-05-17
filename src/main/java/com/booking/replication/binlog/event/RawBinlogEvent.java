package com.booking.replication.binlog.event;

import com.github.shyiko.mysql.binlog.event.*;
import com.google.code.or.binlog.BinlogEventV4;
import com.google.code.or.binlog.impl.event.*;
import com.google.code.or.common.util.MySQLConstants;

/**
 * Generic class for working with binlog events from different parser providers
 */
public class RawBinlogEvent {

    protected RawBinlogEventHeader rawEventHeader; // TODO: use this

    protected final int     BINLOG_PARSER_PROVIDER;
    protected final boolean USING_DEPRECATED_PARSER;

    protected BinlogEventV4 binlogEventV4;
    protected Event         binlogConnectorEvent;

    protected long          timestampOfReceipt;
    protected long          timestampOfBinlogEvent;


    public void setBinlogFilename(String binlogFilename) {
        this.binlogFilename = binlogFilename;
    }

    private String binlogFilename;

    public RawBinlogEvent(Object event) throws Exception {

        // timeOfReceipt in OpenReplicator is set as:
        //
        //       header.setTimestampOfReceipt(System.currentTimeMillis());
        //
        // Since this field does not exists in BinlogConnector and its not
        // really binlog related, but it is time when the parser received the
        // event, we can use here System.currentTimeMillis() and it will
        // represent the time when the producer created this object.
        this.timestampOfReceipt = System.currentTimeMillis();

        if (event instanceof BinlogEventV4) {
            BINLOG_PARSER_PROVIDER = BinlogEventParserProviderCode.OR;

            // this can be done in a nicer way by wraping the twp respective types
            // into wraper objects which have an additional property 'ON/OFF' so
            // we avoid the null assignmetns. Since we use th
            binlogConnectorEvent = null;

            binlogEventV4 = (BinlogEventV4) event;
            timestampOfBinlogEvent = binlogEventV4.getHeader().getTimestamp();

            USING_DEPRECATED_PARSER = true;

        }
        else if (event instanceof Event) {
            BINLOG_PARSER_PROVIDER = BinlogEventParserProviderCode.SHYIKO;

            binlogEventV4 = null;

            binlogConnectorEvent = (Event) event;
            timestampOfBinlogEvent = binlogConnectorEvent.getHeader().getTimestamp();

            USING_DEPRECATED_PARSER = false;
        }
        else {
            throw new Exception("Unsupported parser!");
        }
    }

    public boolean hasHeader() {
        if (binlogEventV4 != null) {
            return (binlogEventV4.getHeader() != null);
        }
        else {
            return (binlogConnectorEvent.getHeader() != null);
        }
    }

    public long getTimestampOfReceipt() {
       return timestampOfReceipt;
    }

    // timestamp received from OpenReplicator is in millisecond form,
    // but the millisecond part is actually 000 (for example 1447755881000)
    // TODO: verify that this is the same in Binlog Connector
    public long getTimestamp() {
        return this.timestampOfBinlogEvent;
    }

    public String getBinlogFilename() {
        if (USING_DEPRECATED_PARSER) {
            return getOpenReplicatorEventBinlogFileName(binlogEventV4);
        }
        else {
            return binlogFilename;
        }
    }

    public void overrideTimestamp(long newTimestampValue) {
        this.timestampOfBinlogEvent = newTimestampValue;
    }

    public boolean isQuery() {
        // All constants from OR are Enums in BinlogConnector so need to check for both
        if (binlogEventV4 != null) {
            return (binlogEventV4.getHeader().getEventType() == MySQLConstants.QUERY_EVENT);
        }
        else {
            return (binlogConnectorEvent.getHeader().getEventType() == EventType.QUERY);
        }
    }

    public String getQuerySQL() {
        if (USING_DEPRECATED_PARSER) {
            return    ((QueryEvent) binlogEventV4).getSql().toString();
        }
        else {
            return ((QueryEventData) binlogConnectorEvent.getData()).getSql();
        }
    }

    public boolean isTableMap() {
        // All constants from OR are Enums in BinlogConnector so need to check for both
        if (binlogEventV4 != null) {
            return (binlogEventV4.getHeader().getEventType() == MySQLConstants.TABLE_MAP_EVENT);
        }
        else {
            return (binlogConnectorEvent.getHeader().getEventType() == EventType.TABLE_MAP);
        }
    }

    public boolean isUpdateRows() {
        // All constants from OR are Enums in BinlogConnector so need to check for both
        if (binlogEventV4 != null) {
            return (
                (binlogEventV4.getHeader().getEventType() == MySQLConstants.UPDATE_ROWS_EVENT_V2)
                ||
                (binlogEventV4.getHeader().getEventType() == MySQLConstants.UPDATE_ROWS_EVENT)
            );
        }
        else {
            return EventType.isUpdate(binlogConnectorEvent.getHeader().getEventType());
        }
    }

    public boolean isWriteRows() {
        if (binlogEventV4 != null) {
            return (
                (binlogEventV4.getHeader().getEventType() == MySQLConstants.WRITE_ROWS_EVENT_V2)
                ||
                (binlogEventV4.getHeader().getEventType() == MySQLConstants.WRITE_ROWS_EVENT)
            );
        }
        else {
            return EventType.isWrite(binlogConnectorEvent.getHeader().getEventType());
        }
    }

    public boolean isDeleteRows() {
        // All constants from OR are Enums in BinlogConnector so need to check for both
        if (binlogEventV4 != null) {
            return (
                (binlogEventV4.getHeader().getEventType() == MySQLConstants.DELETE_ROWS_EVENT_V2)
                ||
                (binlogEventV4.getHeader().getEventType() == MySQLConstants.DELETE_ROWS_EVENT)
            );
        }
        else {
            return EventType.isDelete(binlogConnectorEvent.getHeader().getEventType());
        }
    }

    public boolean  isXid() {
        // All constants from OR are Enums in BinlogConnector so need to check for both
        if (binlogEventV4 != null) {
            return (binlogEventV4.getHeader().getEventType() == MySQLConstants.XID_EVENT);
        }
        else {
            return (binlogConnectorEvent.getHeader().getEventType() == EventType.XID);
        }
    }

    public boolean isFormatDescription() {
        // All constants from OR are Enums in BinlogConnector so need to check for both
        if (binlogEventV4 != null) {
            return (binlogEventV4.getHeader().getEventType() == MySQLConstants.FORMAT_DESCRIPTION_EVENT);
        }
        else {
            return (binlogConnectorEvent.getHeader().getEventType() == EventType.FORMAT_DESCRIPTION);
        }
    }

    public boolean isRotate() {
        // All constants from OR are Enums in BinlogConnector so need to check for both
        if (binlogEventV4 != null) {
            return (binlogEventV4.getHeader().getEventType() == MySQLConstants.ROTATE_EVENT);
        }
        else {
            return (binlogConnectorEvent.getHeader().getEventType() == EventType.ROTATE);
        }
    }

    public boolean isStop() {
        // All constants from OR are Enums in BinlogConnector so need to check for both
        if (binlogEventV4 != null) {
            return (binlogEventV4.getHeader().getEventType() == MySQLConstants.STOP_EVENT);
        }
        else {
            return (binlogConnectorEvent.getHeader().getEventType() == EventType.STOP);
        }
    }

    // TYPE
    public RawEventType getEventType() {

        RawEventType t = RawEventType.UNKNOWN;

        if (binlogEventV4 != null) {
            switch (binlogEventV4.getHeader().getEventType()) {
                case MySQLConstants.QUERY_EVENT:
                    t = RawEventType.QUERY_EVENT;
                    break;
                case MySQLConstants.TABLE_MAP_EVENT:
                    t = RawEventType.TABLE_MAP_EVENT;
                    break;
                case MySQLConstants.UPDATE_ROWS_EVENT:
                case MySQLConstants.UPDATE_ROWS_EVENT_V2:
                    t = RawEventType.UPDATE_ROWS_EVENT;
                    break;
                case MySQLConstants.WRITE_ROWS_EVENT:
                case MySQLConstants.WRITE_ROWS_EVENT_V2:
                    t = RawEventType.WRITE_ROWS_EVENT;
                    break;
                case MySQLConstants.DELETE_ROWS_EVENT:
                case MySQLConstants.DELETE_ROWS_EVENT_V2:
                    t = RawEventType.DELETE_ROWS_EVENT;
                    break;
                case MySQLConstants.XID_EVENT:
                    t = RawEventType.XID_EVENT;
                    break;
                case MySQLConstants.FORMAT_DESCRIPTION_EVENT:
                    t = RawEventType.FORMAT_DESCRIPTION_EVENT;
                    break;
                case MySQLConstants.ROTATE_EVENT:
                    t = RawEventType.ROTATE_EVENT;
                    break;
                case MySQLConstants.STOP_EVENT:
                    break;
                default:
                    t = RawEventType.UNKNOWN;
                    break;
            }
        }
        else {
            switch (binlogConnectorEvent.getHeader().getEventType()) {
                case QUERY:
                    t = RawEventType.QUERY_EVENT;
                    break;
                case TABLE_MAP:
                    t = RawEventType.TABLE_MAP_EVENT;
                    break;
                case PRE_GA_UPDATE_ROWS:
                case UPDATE_ROWS:
                case EXT_UPDATE_ROWS:
                    t = RawEventType.UPDATE_ROWS_EVENT;
                    break;
                case PRE_GA_WRITE_ROWS:
                case WRITE_ROWS:
                case EXT_WRITE_ROWS:
                    t = RawEventType.WRITE_ROWS_EVENT;
                    break;
                case PRE_GA_DELETE_ROWS:
                case DELETE_ROWS:
                case EXT_DELETE_ROWS:
                    t = RawEventType.DELETE_ROWS_EVENT;
                    break;
                case XID:
                    t = RawEventType.XID_EVENT;
                    break;
                case FORMAT_DESCRIPTION:
                    t = RawEventType.FORMAT_DESCRIPTION_EVENT;
                    break;
                case ROTATE:
                    t = RawEventType.ROTATE_EVENT;
                    break;
                case STOP:
                    t = RawEventType.STOP_EVENT;
                    break;
                default:
                    t = RawEventType.UNKNOWN;
                    break;
            }
        }
        return  t;
    }

    // #####################################################
    // FILE NAME
    public String getFilename() {
        if (this.USING_DEPRECATED_PARSER) {
            return getOpenReplicatorEventBinlogFileName(this.binlogEventV4);
        }
        else {
           // since binlog connector only has file name in the rotate event and we need it
           // in other events, we set it in the producer and have it available here.
            return binlogFilename;
        }
    }
    private String getOpenReplicatorEventBinlogFileName(BinlogEventV4 event) {

        switch (event.getHeader().getEventType()) {

            // Query Event:
            case MySQLConstants.QUERY_EVENT:
                return  ((QueryEvent) event).getBinlogFilename();

            // TableMap event:
            case MySQLConstants.TABLE_MAP_EVENT:
                return ((TableMapEvent) event).getBinlogFilename();

            case MySQLConstants.UPDATE_ROWS_EVENT:
            case MySQLConstants.UPDATE_ROWS_EVENT_V2:
            case MySQLConstants.WRITE_ROWS_EVENT:
            case MySQLConstants.WRITE_ROWS_EVENT_V2:
            case MySQLConstants.DELETE_ROWS_EVENT:
            case MySQLConstants.DELETE_ROWS_EVENT_V2:
                return ((AbstractRowEvent) event).getBinlogFilename();

            case MySQLConstants.XID_EVENT:
                return ((XidEvent) event).getBinlogFilename();

            case MySQLConstants.ROTATE_EVENT:
                return ((RotateEvent) event).getBinlogFilename();

            case MySQLConstants.FORMAT_DESCRIPTION_EVENT:
                return ((FormatDescriptionEvent) event).getBinlogFilename();

            case MySQLConstants.STOP_EVENT:
                return ((StopEvent) event).getBinlogFilename();

            default:
                // TODO: handle this on the caller side - default to current pipeline position
                // since it has not changed since its not format or rotate event
                return null;
        }
    }

    // POSITION
    public long getPosition() {
        if (this.USING_DEPRECATED_PARSER) {
            return this.binlogEventV4.getHeader().getPosition();
        }
        else {
            // this EventHeaderV4 implements EventHeader and is returned by Event.getHeader() but
            // needs to be explicitly casted since getHeader returns <T extends EventHeader> T
            // look at
            //      https://github.com/shyiko/mysql-binlog-connector-java/blob/eead0ec2338f7229ba74a7a04188f4967f13d4b7/src/main/java/com/github/shyiko/mysql/binlog/event/Event.java
            // and
            //      https://github.com/shyiko/mysql-binlog-connector-java/blob/eead0ec2338f7229ba74a7a04188f4967f13d4b7/src/main/java/com/github/shyiko/mysql/binlog/event/EventHeaderV4.java
            return ((EventHeaderV4) this.binlogConnectorEvent.getHeader()).getPosition();
        }
    }

    public long getNextPosition() {
        if (this.USING_DEPRECATED_PARSER) {
            return this.binlogEventV4.getHeader().getNextPosition();
        }
        else {

            // this EventHeaderV4 implements EventHeader and is returned by Event.getHeader() but
            // needs to be explicitly casted since getHeader returns <T extends EventHeader> T
            // look at
            //      https://github.com/shyiko/mysql-binlog-connector-java/blob/eead0ec2338f7229ba74a7a04188f4967f13d4b7/src/main/java/com/github/shyiko/mysql/binlog/event/Event.java
            // and
            //      https://github.com/shyiko/mysql-binlog-connector-java/blob/eead0ec2338f7229ba74a7a04188f4967f13d4b7/src/main/java/com/github/shyiko/mysql/binlog/event/EventHeaderV4.java
            return ((EventHeaderV4) this.binlogConnectorEvent.getHeader()).getNextPosition();
        }
    }

    public BinlogEventV4 getBinlogEventV4() {
        return binlogEventV4;
    }

    public Event getBinlogConnectorEvent() {
        return binlogConnectorEvent;
    }
}
