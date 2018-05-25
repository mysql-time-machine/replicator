package com.booking.replication.augmenter.pipeline;

import com.booking.replication.augmenter.pipeline.EventPosition;
import com.google.code.or.binlog.impl.event.TableMapEvent;
import com.google.code.or.binlog.impl.event.XidEvent;
import com.google.code.or.common.util.MySQLConstants;

import com.booking.replication.supplier.model.RawEvent;

import com.booking.replication.supplier.model.TableMapEventData;

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

    private String currentReplicantHostName;
    private int    currentReplicantServerID;

    public PipelinePosition(
            String mySQLHost,
            int    serverID,
            String startingBinlogFilename,
            Long   startingBinlogPosition,
            String lastVerifiedBinlogFilename,
            Long   lastVerifiedBinlogPosition
    ) {
        initPipelinePosition(
                mySQLHost,
                serverID,
                startingBinlogFilename,
                startingBinlogPosition,
                lastVerifiedBinlogFilename,
                lastVerifiedBinlogPosition
        );
    }

    // TODO: use this!
    public PipelinePosition(
            String currentPseudoGTID,
            String currentPseudoGTIDFullQuery,
            String mySQLHost,
            int    serverID,
            String startingBinlogFilename,
            Long   startingBinlogPosition,
            String lastVerifiedBinlogFilename,
            Long   lastVerifiedBinlogPosition
    ) {
        initPipelinePosition(
                mySQLHost,
                serverID,
                startingBinlogFilename,
                startingBinlogPosition,
                lastVerifiedBinlogFilename,
                lastVerifiedBinlogPosition
        );
        this.currentPseudoGTID          = currentPseudoGTID;
        this.currentPseudoGTIDFullQuery = currentPseudoGTIDFullQuery;
    }

    private void initPipelinePosition(
            String mySQLHost,
            int    serverID,
            String startingBinlogFilename,
            Long   startingBinlogPosition,
            String lastVerifiedBinlogFileName,
            Long   lastVerifiedBinlogPosition
    ) {
        this.currentReplicantHostName   = mySQLHost;
        this.currentReplicantServerID   = serverID;

        BinlogPositionInfo startingBinlogPositionInfo;
        startingBinlogPositionInfo = new BinlogPositionInfo(
                mySQLHost,
                serverID,
                startingBinlogFilename,
                startingBinlogPosition
        );

        BinlogPositionInfo lastVerifiedBinlogPositionInfo;
        lastVerifiedBinlogPositionInfo = new BinlogPositionInfo(
                mySQLHost,
                serverID,
                lastVerifiedBinlogFileName,
                lastVerifiedBinlogPosition
        );
        this.setStartPosition(startingBinlogPositionInfo);
        this.setCurrentPosition(startingBinlogPositionInfo); // <- on startup currentPosition := startingPosition
        this.setLastSafeCheckPointPosition(lastVerifiedBinlogPositionInfo);
    }

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

    public BinlogPositionInfo getCurrentPosition() {
        return currentPosition;
    }

    public void setCurrentPosition(BinlogPositionInfo currentPosition) {
        this.currentPosition = currentPosition;
    }

    public BinlogPositionInfo getLastMapEventPosition() {
        return lastMapEventPosition;
    }

    public void setLastMapEventPosition(BinlogPositionInfo lastMapEventPosition) {
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

    public String getCurrentReplicantHostName() {
        return currentReplicantHostName;
    }

    public void setCurrentReplicantHostName(String currentReplicantHostName) {
        this.currentReplicantHostName = currentReplicantHostName;
    }

    public int getCurrentReplicantServerID() {
        return currentReplicantServerID;
    }

    public void setCurrentReplicantServerID(int currentReplicantServerID) {
        this.currentReplicantServerID = currentReplicantServerID;
    }

    public void updatePipelineLastMapEventPosition(
            String host,
            int serverID,
            RawEvent event,
            long fakeMicrosecondCounter
    ) {
        if (this.getLastMapEventPosition() == null) {
            this.setLastMapEventPosition(new BinlogPositionInfo(
                    host,
                    serverID,
                    event.getHeader().getBinlogFileName(),
                    event.getHeader().getBinlogPosition(),
                    fakeMicrosecondCounter
            ));
        } else {
            this.getLastMapEventPosition().setHost(host);
            this.getLastMapEventPosition().setServerID(serverID);
            this.getLastMapEventPosition().setBinlogFilename(EventPosition.getEventBinlogFileName(event));
            this.getLastMapEventPosition().setBinlogPosition(EventPosition.getEventBinlogPosition(event));
            this.getLastMapEventPosition().setFakeMicrosecondsCounter(fakeMicrosecondCounter);
        }
    }

    public void updateCurrentPipelinePosition(
            String host,
            int    serverID,
            String binlogFilename,
            long   binlogPosition,
            long   fakeMicrosecondCounter) {

        this.getCurrentPosition().setHost(host);
        this.getCurrentPosition().setServerID(serverID);
        this.getCurrentPosition().setBinlogFilename(binlogFilename);
        this.getCurrentPosition().setBinlogPosition(binlogPosition);
        this.getCurrentPosition().setFakeMicrosecondsCounter(fakeMicrosecondCounter);
    }

    public void updateCurrentPipelinePosition(
            String   host,
            int      serverID,
            RawEvent event,
            long     fakeMicrosecondCounter
    ) {
        this.getCurrentPosition().setHost(host);
        this.getCurrentPosition().setServerID(serverID);

        // binlog file name is updated on all events and not just on rotate event due to support for
        // mysql failover, so before the rotate event is reached the binlog file name can change in
        // case of mysql failover
        this.getCurrentPosition().setBinlogFilename(event.getHeader().getBinlogFileName());
        this.getCurrentPosition().setBinlogPosition(event.getHeader().getBinlogPosition());
        this.getCurrentPosition().setFakeMicrosecondsCounter(fakeMicrosecondCounter);
    }
}