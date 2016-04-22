package com.booking.replication.queues;

import com.booking.replication.Constants;
import com.google.code.or.binlog.BinlogEventV4;

import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by bosko on 4/20/16.
 */
public class ReplicatorQueues {

    private static final int MAX_RAW_QUEUE_SIZE = Constants.MAX_RAW_QUEUE_SIZE;

    // RawQueue: contains parsed events as extracted by OpenReplicator
    public static final LinkedBlockingQueue<BinlogEventV4> rawQueue =
            new LinkedBlockingQueue<>(MAX_RAW_QUEUE_SIZE);


    // TODO: add RecentCommits Queue that will be used by committedMetrics and for validation
}
