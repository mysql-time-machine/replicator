package com.booking.replication.queues;

import com.booking.replication.Constants;
import com.booking.replication.Metrics;
import com.codahale.metrics.Gauge;
import com.google.code.or.binlog.BinlogEventV4;

import java.util.concurrent.LinkedBlockingQueue;

import static com.codahale.metrics.MetricRegistry.name;

/**
 * Created by bosko on 4/20/16.
 */
public class ReplicatorQueues {
    public ReplicatorQueues() {
        Metrics.registry.register(name("events", "rawEventsQueueLength"),
            new Gauge<Integer>() {
                @Override
                public Integer getValue() {
                    return rawQueue.size();
                }
            });
    }

    private static final int MAX_RAW_QUEUE_SIZE = Constants.MAX_RAW_QUEUE_SIZE;

    // RawQueue: contains parsed events as extracted by OpenReplicator
    public final LinkedBlockingQueue<BinlogEventV4> rawQueue =
            new LinkedBlockingQueue<>(MAX_RAW_QUEUE_SIZE);


    // TODO: add RecentCommits Queue that will be used by committedMetrics and for validation
}
