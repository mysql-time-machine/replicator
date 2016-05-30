package com.booking.replication.checkpoints;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by bosko on 5/30/16.
 */
public class SafeCheckPointFactory {

    private static final Logger LOGGER = LoggerFactory.getLogger(SafeCheckPointFactory.class);

    public static SafeCheckPoint getSafeCheckPoint(int type) {
        if (type == SafeCheckpointType.BINLOG_FILENAME) {
            return new LastVerifiedBinlogFile();
        }
        else if (type == SafeCheckpointType.GTID) {
            LOGGER.warn("GTID safe checkpoint not implemented yet. Defaulting to binlog filename.");
            return new LastVerifiedBinlogFile();
        }
        else {
            LOGGER.error("Unknown SafeCheckPoint type");
            System.exit(-1);
        }
        return  null;
    }

    public SafeCheckPoint getSafeCheckPointFromJSON(String json) {
        ObjectMapper mapper = new ObjectMapper();
        String jsonInString = json;
        SafeCheckPoint checkpoint = null;
        try {
            checkpoint = mapper.readValue(json, SafeCheckPoint.class);
        }
        catch (Exception e) {
            LOGGER.error("Could not deserialize object.",e);
            System.exit(-1);
        }
        return checkpoint;
    }
}
