package com.booking.replication.checkpoints;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created by bosko on 5/30/16.
 */
public class LastCommittedPositionCheckpoint implements SafeCheckPoint {

    private static final Logger LOGGER = LoggerFactory.getLogger(LastCommittedPositionCheckpoint.class);

    private final int checkpointType = SafeCheckpointType.BINLOG_POSITION;

    private String hostName;
    private int    slaveId;
    private String lastVerifiedBinlogFileName;
    private long   lastVerifiedBinlogPosition = 4L;

    // this is only for tracking purposes. The HA pGTID safe checkpoint will
    // be implemented in different class
    private String pseudoGTID;
    private String pseudoGTIDFullQuery;

    public LastCommittedPositionCheckpoint() {}

    public LastCommittedPositionCheckpoint(int slaveId, String binlogFileName) {
        this(slaveId, binlogFileName, 4L);
    }

    /**
     * Represents the last processed binlog file with last commited position.
     *
     * @param slaveId           Id of the slave that originated the binlog.
     * @param binlogFileName    File name
     * @param binlogPosition    File position
     */
    public LastCommittedPositionCheckpoint(int slaveId, String binlogFileName, long binlogPosition) {
        this.slaveId = slaveId;
        lastVerifiedBinlogFileName = binlogFileName;
        lastVerifiedBinlogPosition = binlogPosition;
    }

    /**
     * Represents the last processed binlog file with last commited postion and pGTID.
     *
     * @param hostName            Host name of the mysql host that originated the binlog
     * @param slaveId             Server Id of the mysql host that originated the binlog
     * @param binlogFileName      File name
     * @param binlogPosition      File position
     * @param pseudoGTID          Pseudo GTID identifier extracted from full pGTID query
     * @param pseudoGTIDFullQuery Pseudo GTID Full Query
     */
    public LastCommittedPositionCheckpoint(
        String hostName,
        int slaveId,
        String binlogFileName,
        long binlogPosition,
        String pseudoGTID,
        String pseudoGTIDFullQuery
    ) {
        this.hostName                   = hostName;
        this.slaveId                    = slaveId;
        this.lastVerifiedBinlogFileName = binlogFileName;
        this.lastVerifiedBinlogPosition = binlogPosition;
        this.pseudoGTID                 = pseudoGTID;
        this.pseudoGTIDFullQuery        = pseudoGTIDFullQuery;
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
