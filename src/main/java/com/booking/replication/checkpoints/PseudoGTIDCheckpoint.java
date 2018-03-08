package com.booking.replication.checkpoints;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class PseudoGTIDCheckpoint implements SafeCheckPoint {

    private static final Logger LOGGER = LoggerFactory.getLogger(PseudoGTIDCheckpoint.class);

    private final int checkpointType;

    private String hostName;
    private int    slaveId;
    private String lastVerifiedBinlogFileName;
    private long   lastVerifiedBinlogPosition = 4L;

    private String pseudoGTID;
    private String pseudoGTIDFullQuery;
    private long fakeMicrosecondCounter = 0L;


//    public PseudoGTIDCheckpoint() {
  //      checkpointType = SafeCheckpointType.BINLOG_POSITION;
   // }

    /**
     * Represents the pseudoGTID checkpoint.
     *
     * @param hostName            Host name of the mysql host that originated the binlog
     * @param slaveId             Server Id of the mysql host that originated the binlog
     * @param binlogFileName      File name
     * @param binlogPosition      File position
     * @param pseudoGTID          PseudoGTID identifier extracted from full pseudoGTID query
     * @param pseudoGTIDFullQuery PseudoGTID Full Query
     */
    public PseudoGTIDCheckpoint(
        String hostName,
        int    slaveId,
        String binlogFileName,
        long   binlogPosition,
        String pseudoGTID,
        String pseudoGTIDFullQuery,
        long   fakeMicrosecondCounter
    ) {
        this.hostName                   = hostName;
        this.slaveId                    = slaveId;
        this.lastVerifiedBinlogFileName = binlogFileName;
        this.lastVerifiedBinlogPosition = binlogPosition;
        this.pseudoGTID                 = pseudoGTID;
        this.pseudoGTIDFullQuery        = pseudoGTIDFullQuery;
        this.checkpointType             = SafeCheckpointType.GLOBAL_PSEUDO_GTID;
        this.fakeMicrosecondCounter     = fakeMicrosecondCounter;
    }

    @Override
    public int getCheckpointType() {
        return this.checkpointType;
    }

    public Long getLastVerifiedBinlogPosition() {
        return  lastVerifiedBinlogPosition;
    }

    public int getSlaveId() {
        return slaveId;
    }

    public String getLastVerifiedBinlogFileName() {
        return lastVerifiedBinlogFileName;
    }

    public String getPseudoGTID() {
        return pseudoGTID;
    }

    public long getFakeMicrosecondCounter() {
        return fakeMicrosecondCounter;
    }

    public String getHostName() {
        return hostName;
    }

    public String getPseudoGTIDFullQuery() {
        return pseudoGTIDFullQuery;
    }

    // setters: needed for deserialization
    public void setHostName(String hostName) {
        this.hostName = hostName;
    }

    public void setSlaveId(int slaveId) {
        this.slaveId = slaveId;
    }

    public void setLastVerifiedBinlogFileName(String lastVerifiedBinlogFileName) {
        this.lastVerifiedBinlogFileName = lastVerifiedBinlogFileName;
    }

    public void setLastVerifiedBinlogPosition(long lastVerifiedBinlogPosition) {
        this.lastVerifiedBinlogPosition = lastVerifiedBinlogPosition;
    }

    public void setPseudoGTID(String pseudoGTID) {
        this.pseudoGTID = pseudoGTID;
    }

    public void setPseudoGTIDFullQuery(String pseudoGTIDFullQuery) {
        this.pseudoGTIDFullQuery = pseudoGTIDFullQuery;
    }

    private ObjectMapper mapper = new ObjectMapper();

    public boolean isBeforeCheckpoint(PseudoGTIDCheckpoint checkpointToCompareTo) {
        // next pseudoGTID is lexicographically greater than the previous one
        return (!this.getPseudoGTID().equals(checkpointToCompareTo.getPseudoGTID()));
    }

    @Override
    public String toJson() {
        String json = null;
        try {
            json = mapper.writeValueAsString(this);
        } catch (IOException e) {
            LOGGER.error("ERROR: could not serialize event", e);
        }
        return json;
    }
}
