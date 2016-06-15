package com.booking.replication.queues;

import static com.codahale.metrics.MetricRegistry.name;

import com.booking.replication.Constants;
import com.booking.replication.Metrics;
import com.google.code.or.binlog.BinlogEventV4;

import com.codahale.metrics.Gauge;

import java.util.concurrent.LinkedBlockingQueue;

/**
 * This class is a repository for the queues used by the replicators Pipeline Orchestrator.
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
