package com.booking.replication.supplier.mysql.binlog.gtid;

import com.booking.replication.commons.checkpoint.Checkpoint;

import java.util.*;

public class GtidSetAlgebra {

    private final Map<String, Set<String>> serverTransactionRanges;
    private final  Map<String, Set<Long>> serverTransactionUpperLimits;
    private final  Map<String, Map<Long, String>> serverTransactionUpperLimitToRange;
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

        Map<String, Set<Long>> filteredGTIDSets = getSafeGTIDSetForApplierCommitedTransactions();

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
        String safeGtidSet = "";
        StringJoiner sj = new StringJoiner(",");
        for (String serverId: last.keySet()) {
            safeGtidSet += serverId;
            safeGtidSet += ":";
            safeGtidSet += last.get(serverId);
            sj.add(safeGtidSet);
            safeGtidSet = "";
        }
        return  sj.toString();
    }

    private String sortGTIDSet(String gtidSet) {
        String[] items = gtidSet.split(",");
        Arrays.sort(items);
        StringJoiner sj = new StringJoiner(",");
        for (String item: items) {
            sj.add(item);
        }
        return  sj.toString();
    }

    private  Map<String, String> extractFinalRanges(Map<String, Set<Long>> filteredGTIDSets) {

        Map<String, String> last = new HashMap<>();
        for (String serverId : filteredGTIDSets.keySet() ) {
            TreeSet<Long> upperLimits = (TreeSet<Long>) filteredGTIDSets.get(serverId);
            String lastRange =  serverTransactionUpperLimitToRange.get(serverId).get(upperLimits.last());
            last.put(
                    serverId,
                    lastRange
            );
        }
        return last;
    }

    private Long getRangeUpperLimit(String range) {
        return  Long.parseLong(range.split("-")[1]);
    }

    public void addGTIDSetToServersTransactionRangeMap(String gtidSet) {

        String [] serverRanges = gtidSet.split(",");

        for (String serverRange: serverRanges) {
            String[] pair = serverRange.split(":");
            String serverUUID = pair[0];
            String transactionRange = pair[1];

            if (serverTransactionRanges.get(serverUUID) == null) {
                serverTransactionRanges.put(serverUUID, new HashSet<>());
            }
            serverTransactionRanges.get(serverUUID).add(transactionRange);

            if (serverTransactionUpperLimits.get(serverUUID) == null) {
                serverTransactionUpperLimits.put(serverUUID, new TreeSet<>());
            }
            serverTransactionUpperLimits.get(serverUUID).add(getRangeUpperLimit(transactionRange));

            if (serverTransactionUpperLimitToRange.get(serverUUID) == null) {
                serverTransactionUpperLimitToRange.put(serverUUID, new HashMap<>());
            }
            serverTransactionUpperLimitToRange.get(serverUUID).put(
                    getRangeUpperLimit(transactionRange),
                    transactionRange
            );
        }
    }

    // remove the gaps and all transactions higher than the gap beginning
    private Map<String, Set<Long>> getSafeGTIDSetForApplierCommitedTransactions() {
        Map<String, Set<Long>> uninteruptedGTIDRanges = new HashMap<>();
        for (String server : serverTransactionRanges.keySet()) {
            uninteruptedGTIDRanges.put(
                    server,
                    getMaxUninteruptedRangeStartingFromMinimalTransaction(server)
            );
        }

        return  uninteruptedGTIDRanges;
    }


    private TreeSet<Long> getMaxUninteruptedRangeStartingFromMinimalTransaction (String serverId) {

        TreeSet<Long> range = (TreeSet) serverTransactionUpperLimits.get(serverId);
        TreeSet<Long> uninteruptedRange = new TreeSet<>();

        Long position = range.first();

        for (Long item : range) {
            if ((item >= position)) {
                if (item - position == 1) {
                    position = item;
                } else {
                    // gap
                }
            } else {
                throw new RuntimeException("Error in logic");
            }
        }

        Long item = range.pollFirst();
        while (item != null && item <= position) {
            uninteruptedRange.add(item);
            item = range.pollFirst();
        }

        return uninteruptedRange;
    }

}
