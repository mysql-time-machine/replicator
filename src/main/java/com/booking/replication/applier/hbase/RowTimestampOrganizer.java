package com.booking.replication.applier.hbase;

import com.booking.replication.augmenter.AugmentedRow;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This class takes care that all HBase rows with same row_id in same table have different timestamp.
 *
 * Sometimes one may observe long-lasting transactions with query time few minutes before commit time
 * So writing data with query time may lead to situation that
 *   data would be seen existing some time before it really existed
 * So we want to write replicated changes with time of transaction commit - not time of query
 *
 * Setting one single time to all events in transaction is dangerous for a case when there are several
 *   modifications of one cell in a transaction
 * When all of these changes have same timestamp only one (one that was written last) will be stored in HBase
 * So we may loose fact of changes
 * Or because of asynchronous processing event written last will be not really last so we will have differences
 *   between source data and final version of HBase data
 *
 * We want to avoid this situation
 * So we want to set different timestamps to different events in single transaction.
 * If transaction is very big then setting different timestamp to every event is risky
 *   because it may cause transactions overlapping
 * So we would like to change timestamps to different for every row: primary-key and table combination
 *
 * To achieve this we put 1st row for every table/pk combination SPAN (50 in current implementation)
 *   microseconds before finish timestamp.
 * Every next event for this table/PK combination will be put 1 microsecond later
 * With exception when reaching SPAN (50 in current implementation) events when all later ones
 *   will have timestamp equal to finish event
 *
 * So here we assume all event timestamps in transaction were set to finish events.
 * We support up to 50 changes for 1 table/PK combinations in 1 transaction
 */
public class RowTimestampOrganizer {

    private HBaseApplierMutationGenerator hBaseApplierMutationGenerator;

    public RowTimestampOrganizer(HBaseApplierMutationGenerator mgen) {
        this.hBaseApplierMutationGenerator = mgen;
    }

    private class TimestampTuple {
        public long timestamp;
        public long maximumTimestamp;
        public TimestampTuple(long timestamp, long maximumTimestamp) {
            this.timestamp = timestamp;
            this.maximumTimestamp = maximumTimestamp;
        }
    }
    private static final long TIMESTAMP_SPAN_MICROSECONDS = 50;

    private String currentTransactionUUID = null;
    private Map<String, TimestampTuple> timestampsCache;

    public void organizeTimestamps(List<AugmentedRow> rows, String mysqlTableName, String transactionUUID) {
        if (currentTransactionUUID == null ||
                !currentTransactionUUID.equals(transactionUUID)) {
            currentTransactionUUID = transactionUUID;
            timestampsCache = new HashMap<>();
        }
        for (AugmentedRow row : rows) {
            String key = mysqlTableName + ":" + hBaseApplierMutationGenerator.getHBaseRowKey(row);
            TimestampTuple v;
            if (timestampsCache.containsKey(key)) {
                v = timestampsCache.get(key);
                if (v.timestamp < v.maximumTimestamp) {
                    v.timestamp++;
                }
            } else {
                v = new TimestampTuple(
                    row.getCommitTimestamp() - TIMESTAMP_SPAN_MICROSECONDS,
                    row.getCommitTimestamp()
                );
                timestampsCache.put(key, v);
            }
            row.setRowMicrosecondTimestamp(v.timestamp);
        }
    }
}