package com.booking.replication.checkpoints;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created by bosko on 5/30/16.
 */
public class LastVerifiedBinlogFile implements SafeCheckPoint {

    private static final Logger LOGGER = LoggerFactory.getLogger(LastVerifiedBinlogFile.class);

    private final int checkpointType = SafeCheckpointType.BINLOG_FILENAME;

    private String lastVerifiedBinlogFileName;

    @Override
    public int getCheckpointType() {
        return this.checkpointType;
    }

    @Override
    public String getSafeCheckPointMarker() {
        return lastVerifiedBinlogFileName;
    }

    @Override
    public void setSafeCheckPointMarker(String marker) {
        lastVerifiedBinlogFileName = marker;
        LOGGER.info("SafeCheckPoint marter set to: " + lastVerifiedBinlogFileName);
    }

    @Override
    public String toJSON() {
        String json = null;
        try {
            ObjectMapper mapper = new ObjectMapper();
            json = mapper.writeValueAsString(this);
        } catch (IOException e) {
            LOGGER.error("ERROR: could not serialize event", e);
        }
        return json;
    }
}
