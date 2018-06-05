package com.booking.replication.pipeline;

import com.booking.replication.binlog.EventPosition;
import com.booking.replication.binlog.event.impl.BinlogEventTableMap;
import com.booking.replication.binlog.event.IBinlogEvent;

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
            BinlogEventTableMap event,
            long fakeMicrosecondCounter
    ) {
        if (this.getLastMapEventPosition() == null) {
            this.setLastMapEventPosition(new BinlogPositionInfo(
                    host,
                    serverID,
                    event.getBinlogFilename(),
                    event.getPosition(),
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
            int serverID,
            String binlogFilename,
            long binlogPosition,
            long fakeMicrosecondCounter) {

        this.getCurrentPosition().setHost(host);
        this.getCurrentPosition().setServerID(serverID);
        this.getCurrentPosition().setBinlogFilename(binlogFilename);
        this.getCurrentPosition().setBinlogPosition(binlogPosition);
        this.getCurrentPosition().setFakeMicrosecondsCounter(fakeMicrosecondCounter);
    }


    public void updatCurrentPipelinePosition(
        String host,
        int serverID,
        IBinlogEvent event,
        long fakeMicrosecondCounter
    ) {
        this.getCurrentPosition().setHost(host);
        this.getCurrentPosition().setServerID(serverID);

        // binlog file name is updated on all events and not just on rotate event due to support for
        // mysql failover, so before the rotate event is reached the binlog file name can change in
        // case of mysql failover
        this.getCurrentPosition().setBinlogFilename(event.getBinlogFilename());
        this.getCurrentPosition().setBinlogPosition(event.getPosition());
        this.getCurrentPosition().setFakeMicrosecondsCounter(fakeMicrosecondCounter);
    }

}
