package com.booking.replication.mysql;

import java.util.Iterator;
import java.util.List;

/**
 * Created by bosko on 9/12/16.
 */
public class ReplicantPool {

    private final List<String> replicantPool;

    public ReplicantPool(List<String> replicantPool) {
        this.replicantPool = replicantPool;
    }

    public String getReplicantDBActiveHost() {
        // TODO
        return replicantPool.get(0);
    }

    public int getReplicantDBActiveHostServerID() {
        //TODO
        return 1;
    }

    public String getActiveHost() throws Exception {

        boolean foundGoodHost = false;
        String activeHost = null;
        Iterator<String> hostIterator = replicantPool.iterator();

        while (!foundGoodHost) {
            if (hostIterator.hasNext()) {
                String host = hostIterator.next();
                if (isGood(host)) {
                    activeHost = host;
                    foundGoodHost = true;
                }
            } else {
                throw new Exception("Replicant pool depleted, no available hosts found!");
            }
        }
        return activeHost;
    }

    private boolean isGood(String host) {
        // TODO
        return true;
    }
}
