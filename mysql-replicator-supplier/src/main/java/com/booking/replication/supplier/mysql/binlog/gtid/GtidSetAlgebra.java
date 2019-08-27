package com.booking.replication.supplier.mysql.binlog.gtid;

import com.booking.replication.commons.checkpoint.Checkpoint;

import java.util.*;

public class GtidSetAlgebra {

    private final Map<String, Set<String>> serverTransactionRanges;
    private final Map<String, Set<Long>> serverTransactionUpperLimits;
    private final Map<String, Map<Long, String>> serverTransactionUpperLimitToRange;
    private final Map<String, Checkpoint> gtidSetToCheckpoint;

    public GtidSetAlgebra() {
        this.serverTransactionRanges = new TreeMap<>();
        this.serverTransactionUpperLimits = new TreeMap<>();
        this.serverTransactionUpperLimitToRange = new TreeMap<>();
        this.gtidSetToCheckpoint = new TreeMap<>();
    }

    public synchronized Checkpoint getSafeCheckpoint(List<Checkpoint> checkpointsSeenWithGtidSet) {

        Checkpoint safeCheckpoint;

        for (Checkpoint checkpoint : checkpointsSeenWithGtidSet) {
            String seenGTIDSet = sortGTIDSet(checkpoint.getGtidSet());
            gtidSetToCheckpoint.put(seenGTIDSet, checkpoint);
            addGTIDSetToServersTransactionRangeMap(seenGTIDSet);
        }

        Map<String, Set<Long>> filteredGTIDSets = getSafeGTIDSetForApplierCommittedTransactions();

        Map<String, String> last = extractFinalRanges(filteredGTIDSets);

        String safeGTIDSet = sortGTIDSet(getSafeGTIDSet(last));

        safeCheckpoint = new Checkpoint(
            gtidSetToCheckpoint.get(safeGTIDSet).getTimestamp(),
            gtidSetToCheckpoint.get(safeGTIDSet).getServerId(),
            gtidSetToCheckpoint.get(safeGTIDSet).getGtid(),
            gtidSetToCheckpoint.get(safeGTIDSet).getBinlog(),
            gtidSetToCheckpoint.get(safeGTIDSet).getGtidSet()
        );

        serverTransactionUpperLimitToRange.clear();
        serverTransactionUpperLimits.clear();
        serverTransactionRanges.clear();
        gtidSetToCheckpoint.clear();

        return safeCheckpoint;
    }

    private String getSafeGTIDSet(Map<String, String> last) {
        final StringJoiner sj = new StringJoiner(",");
        for (Map.Entry<String, String> entry : last.entrySet()) {
            sj.add(entry.getKey() + ":" + entry.getValue());
        }
        return sj.toString();
    }

    private String sortGTIDSet(String gtidSet) {
        String[] items = gtidSet.split(",");
        Arrays.sort(items);
        StringJoiner sj = new StringJoiner(",");
        for (String item : items) {
            sj.add(item);
        }
        return sj.toString();
    }

    private Map<String, String> extractFinalRanges(Map<String, Set<Long>> filteredGTIDSets) {
        Map<String, String> last = new HashMap<>();
        for (Map.Entry<String, Set<Long>> entry : filteredGTIDSets.entrySet()) {
            final String serverId = entry.getKey();
            final String lastRange = serverTransactionUpperLimitToRange
                .get(serverId)
                .get(((TreeSet<Long>) entry.getValue()).last());
            last.put(serverId, lastRange);
        }
        return last;
    }

    private Long getRangeUpperLimit(String range) {
        return Long.parseLong(range.split("-")[1]);
    }

    public void addGTIDSetToServersTransactionRangeMap(String gtidSet) {

        String[] serverRanges = gtidSet.split(",");

        for (String serverRange : serverRanges) {
            String[] pair = serverRange.split(":");
            String serverUUID = pair[0];
            String transactionRange = pair[1];

            serverTransactionRanges.computeIfAbsent(serverUUID, k -> new HashSet<>());
            serverTransactionRanges.get(serverUUID).add(transactionRange);

            serverTransactionUpperLimits.computeIfAbsent(serverUUID, k -> new TreeSet<>());
            serverTransactionUpperLimits.get(serverUUID).add(getRangeUpperLimit(transactionRange));

            serverTransactionUpperLimitToRange.computeIfAbsent(serverUUID, k -> new HashMap<>());
            serverTransactionUpperLimitToRange.get(serverUUID).put(
                getRangeUpperLimit(transactionRange),
                transactionRange
            );
        }
    }

    // remove the gaps and all transactions higher than the gap beginning
    private Map<String, Set<Long>> getSafeGTIDSetForApplierCommittedTransactions() {
        Map<String, Set<Long>> uninterruptedGTIDRanges = new HashMap<>();
        for (String server : serverTransactionRanges.keySet()) {
            uninterruptedGTIDRanges.put(
                server,
                getMaxUninteruptedRangeStartingFromMinimalTransaction(server)
            );
        }

        return uninterruptedGTIDRanges;
    }


    private TreeSet<Long> getMaxUninteruptedRangeStartingFromMinimalTransaction(String serverId) {
        TreeSet<Long> range = (TreeSet) serverTransactionUpperLimits.get(serverId);
        TreeSet<Long> uninterruptedRange = new TreeSet<>();

        Long position = range.first();

        for (Long item : range) {
            if ((item >= position)) {
                if (item - position == 1) {
                    position = item;
                }
            } else {
                throw new RuntimeException("Error in logic");
            }
        }

        Long item = range.pollFirst();
        while (item != null && item <= position) {
            uninterruptedRange.add(item);
            item = range.pollFirst();
        }

        return uninterruptedRange;
    }

}
