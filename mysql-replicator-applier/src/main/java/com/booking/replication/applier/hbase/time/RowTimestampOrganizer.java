package com.booking.replication.applier.hbase.time;

import static com.booking.replication.applier.hbase.schema.HBaseRowKeyMapper.getSaltedHBaseRowKey;

import com.booking.replication.augmenter.model.row.AugmentedRow;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * class: RowTimestampOrganizer
 *
 *<p>This class takes care that all HBase rows with same row_id in same table have different timestamp.
 *
 *<p>Sometimes one may observe long-lasting transactions with query time few minutes before commit time
 * So writing data with query time may lead to situation that data would be seen existing some time
 * before it really existed. To solve this, we need to write replicated changes with the time of
 * transaction commit - not time of the query.
 *
 *<p>On the other hand, setting one single time to all events in transaction is dangerous for a case
 * when there are several modifications to one cell in the transaction.
 * When all of these changes have same timestamp, only one (one that was written last) will be stored
 * in HBase, so we lose previous changes.
 * Another case is that of the event written last to HBase is not always last chronologically, so
 * we would end up having differences between source data and final version of HBase data
 *
 *<p>The way we avoid this situation is:
 *
 *<li>      We shift the timestamp of a 1st row for every table/pk combination to be a certain interval of
 *<li>      microseconds before the commit_timestamp. Default side of the this SPAN interval is 50 microseconds.
 *
 *<p>Every next event for this table/PK combination will be put 1 microsecond later, with the exception when
 * reaching the end of the SPAN interval (50 micros in current implementation), events after that one will
 * all have timestamp equal to the finish event
 *
 *<p>Here we assume that all events timestamps in transaction were already set to commit_time in the previous
 * steps in the pipeline.
 *
 *<p>We support up to 50 changes for 1 table/PK combinations in 1 transaction
 */
public class RowTimestampOrganizer {

    private static final Logger LOG = LogManager.getLogger(RowTimestampOrganizer.class);

    private class TimestampTuple {
        public long timestamp;
        public long maximumTimestamp;

        public TimestampTuple(long timestamp, long maximumTimestamp) {
            this.timestamp = timestamp;
            this.maximumTimestamp = maximumTimestamp;
        }
    }

    public static final long TIMESTAMP_SPAN_MICROSECONDS = 50;
    private ThreadLocal<String> currentTransactionUUID = new ThreadLocal<>();
    private ConcurrentHashMap<Long, Map<String, TimestampTuple>> timestampsCache = new ConcurrentHashMap<>();

    public void organizeTimestamps(List<AugmentedRow> rows, String mysqlTableName, Long threadID, String transactionUUID) {

        if ( timestampsCache.get(threadID) == null ) {
            timestampsCache.put( threadID, new HashMap<>() );
        }
        // if first call, or we're on a new transaction
        if (currentTransactionUUID.get() == null || !currentTransactionUUID.get().equals(transactionUUID)) {
            currentTransactionUUID.set(transactionUUID);
            timestampsCache.put(threadID, new HashMap<>() );
        }

        for (AugmentedRow row : rows) {
            String key = mysqlTableName + ":" + getSaltedHBaseRowKey(row);
            TimestampTuple timestampTuple;
            if (timestampsCache.get(threadID).containsKey(key)) {
                timestampTuple = timestampsCache.get(threadID).get(key);
                if (timestampTuple.timestamp < timestampTuple.maximumTimestamp) {
                    timestampTuple.timestamp++;
                }
            } else {
                timestampTuple = new TimestampTuple(
                        row.getRowMicrosecondTimestamp() - TIMESTAMP_SPAN_MICROSECONDS,
                        row.getRowMicrosecondTimestamp() // <- maximumTimestamp
                );
                timestampsCache.get(threadID).put(key, timestampTuple);
            }

            printLogger("Before", row);

            row.setRowMicrosecondTimestamp(timestampTuple.timestamp);

            printLogger("After", row);
        }
    }

    private static void printLogger(String prefix, AugmentedRow row) {
        LOG.debug(String.format("[%s] table : %s, UUID: %s, ts: %d", prefix, row.getTableName(),
                row.getTransactionUUID(), row.getRowMicrosecondTimestamp()));
    }
}
