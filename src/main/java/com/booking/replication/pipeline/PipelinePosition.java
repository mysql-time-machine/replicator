package com.booking.replication.pipeline;

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
    private long   currentReplicantServerID;

    public PipelinePosition(
        String mySQLHost,
        long    serverID,
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

    public PipelinePosition(
        String currentPseudoGTID,
        String currentPseudoGTIDFullQuery,
        String mySQLHost,
        long    serverID,
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
            long serverID,
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

    public long getCurrentReplicantServerID() {
        return currentReplicantServerID;
    }

    public void setCurrentReplicantServerID(int currentReplicantServerID) {
        this.currentReplicantServerID = currentReplicantServerID;
    }

}
