package com.booking.replication.pipeline;

/**
 * Created by bosko on 7/25/16.
 */
public class PipelinePosition {

    private com.booking.replication.pipeline.BinlogPositionInfo currentPosition;
    private com.booking.replication.pipeline.BinlogPositionInfo lastMapEventPosition;

    public com.booking.replication.pipeline.BinlogPositionInfo getCurrentPosition() {
        return currentPosition;
    }

    public void setCurrentPosition(com.booking.replication.pipeline.BinlogPositionInfo currentPosition) {
        this.currentPosition = currentPosition;
    }

    public com.booking.replication.pipeline.BinlogPositionInfo getLastMapEventPosition() {
        return lastMapEventPosition;
    }

    public void setLastMapEventPosition(com.booking.replication.pipeline.BinlogPositionInfo lastMapEventPosition) {
        this.lastMapEventPosition = lastMapEventPosition;
    }
}
