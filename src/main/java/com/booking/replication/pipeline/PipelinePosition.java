package com.booking.replication.pipeline;

import com.google.code.or.binlog.BinlogEventV4;
import com.google.code.or.binlog.impl.event.AbstractRowEvent;
import com.google.code.or.binlog.impl.event.FormatDescriptionEvent;
import com.google.code.or.binlog.impl.event.QueryEvent;
import com.google.code.or.binlog.impl.event.RotateEvent;
import com.google.code.or.binlog.impl.event.StopEvent;
import com.google.code.or.binlog.impl.event.TableMapEvent;
import com.google.code.or.binlog.impl.event.XidEvent;
import com.google.code.or.common.util.MySQLConstants;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by bosko on 7/25/16.
 */
public class PipelinePosition {

    private BinlogPositionInfo lastSafeCheckPointPosition;
    private BinlogPositionInfo startPosition;
    private BinlogPositionInfo currentPosition;
    private BinlogPositionInfo lastMapEventPosition;

    private String currentPseudoGTID;
    private String currentPseudoGTIDFullQuery;

    private static final Logger LOGGER = LoggerFactory.getLogger(PipelinePosition.class);

    public String getCurrentPseudoGTID() {
        return currentPseudoGTID;
    }

    public void setCurrentPseudoGTID(String currentPseudoGTID) {
        this.currentPseudoGTID = currentPseudoGTID;
    }

    public String getCurrentPseudoGTIDFullQuery() {
        return currentPseudoGTIDFullQuery;
    }

    public void setCurrentPseudoGTIDFullQuery(String currentPseudoGTIDFullQuery) {
        this.currentPseudoGTIDFullQuery = currentPseudoGTIDFullQuery;
    }

    public com.booking.replication.pipeline.BinlogPositionInfo getCurrentPosition() {
        return currentPosition;
    }

    public void setCurrentPosition(com.booking.replication.pipeline.BinlogPositionInfo currentPosition) {
        this.currentPosition = currentPosition;
    }

    public BinlogPositionInfo getLastMapEventPosition() {
        return lastMapEventPosition;
    }

    public void setLastMapEventPosition(com.booking.replication.pipeline.BinlogPositionInfo lastMapEventPosition) {
        this.lastMapEventPosition = lastMapEventPosition;
    }

    public BinlogPositionInfo getLastSafeCheckPointPosition() {
        return lastSafeCheckPointPosition;
    }

    public void setLastSafeCheckPointPosition(BinlogPositionInfo lastSafeCheckPointPosition) {
        this.lastSafeCheckPointPosition = lastSafeCheckPointPosition;
    }

    public BinlogPositionInfo getStartPosition() {
        return startPosition;
    }

    public void setStartPosition(BinlogPositionInfo startPosition) {
        this.startPosition = startPosition;
    }

    public void updatePipelineLastMapEventPosition(
        String host,
        int serverID,
        TableMapEvent event,
        long fakeMicrosecondCounter
    ) {
        if (this.getLastMapEventPosition() == null) {
            this.setLastMapEventPosition(new BinlogPositionInfo(
                    host,
                    serverID,
                    event.getBinlogFilename(),
                    event.getHeader().getPosition(),
                    fakeMicrosecondCounter
            ));
        } else {
            this.getLastMapEventPosition().setHost(host);
            this.getLastMapEventPosition().setServerID(serverID);
            this.getLastMapEventPosition().setBinlogFilename(getEventBinlogFileName(event));
            this.getLastMapEventPosition().setBinlogPosition(getEventBinlogPosition(event));
            this.getLastMapEventPosition().setFakeMicrosecondsCounter(fakeMicrosecondCounter);
        }
    }

    public void updatCurrentPipelinePosition(
        String host,
        int serverID,
        BinlogEventV4 event,
        long fakeMicrosecondCounter
    ) {
        this.getCurrentPosition().setHost(host);
        this.getCurrentPosition().setServerID(serverID);
        this.getCurrentPosition().setBinlogFilename(getEventBinlogFileName(event));
        this.getCurrentPosition().setBinlogPosition(getEventBinlogPosition(event));
        this.getCurrentPosition().setFakeMicrosecondsCounter(fakeMicrosecondCounter);
    }

    private String getEventBinlogFileName(BinlogEventV4 event) {

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
                LOGGER.warn("Unexpected event type => " + event.getHeader().getEventType());
                // since it's not rotate event or format description event, the binlog file
                // has not changed, so return the last recorded
                return this.getCurrentPosition().getBinlogFilename();
        }
    }

    private long getEventBinlogPosition(BinlogEventV4 event) {

        switch (event.getHeader().getEventType()) {

            // Query Event:
            case MySQLConstants.QUERY_EVENT:
                return  ((QueryEvent) event).getHeader().getPosition();

            // TableMap event:
            case MySQLConstants.TABLE_MAP_EVENT:
                return ((TableMapEvent) event).getHeader().getPosition();

            case MySQLConstants.UPDATE_ROWS_EVENT:
            case MySQLConstants.UPDATE_ROWS_EVENT_V2:
            case MySQLConstants.WRITE_ROWS_EVENT:
            case MySQLConstants.WRITE_ROWS_EVENT_V2:
            case MySQLConstants.DELETE_ROWS_EVENT:
            case MySQLConstants.DELETE_ROWS_EVENT_V2:
                return ((AbstractRowEvent) event).getHeader().getPosition();

            case MySQLConstants.XID_EVENT:
                return ((XidEvent) event).getHeader().getPosition();

            case MySQLConstants.ROTATE_EVENT:
                return ((RotateEvent) event).getHeader().getPosition();

            case MySQLConstants.FORMAT_DESCRIPTION_EVENT:
                // workaround for a bug in open replicator which sets next position to 0, so
                // position turns out to be negative. Since it is always 4 for this event type,
                // we just use 4.
                return 4L;

            case MySQLConstants.STOP_EVENT:
                return ((StopEvent) event).getHeader().getPosition();

            default:
                LOGGER.warn("Unexpected event type: " + event.getHeader().getEventType());
                return event.getHeader().getPosition();
        }
    }
}
