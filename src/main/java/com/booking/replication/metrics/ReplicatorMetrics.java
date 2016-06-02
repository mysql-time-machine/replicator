package com.booking.replication.metrics;

import com.booking.replication.Configuration;

import java.math.BigInteger;
import java.util.*;


/**
 * Created by bosko on 1/19/16.
 */
public class ReplicatorMetrics {

    private final Totals totals;
    private final HashMap<String, RowTotals> totalsPerTable;
    private final HashMap<Integer, TotalsPerTimeSlot> replicatorMetrics;
    private final Object criticalSection = new Object();

    private final Integer beginTime;

    public ReplicatorMetrics(List<String> deltaTables) {

        beginTime = (int) (System.currentTimeMillis() / 1000L);
        replicatorMetrics = new HashMap<Integer, TotalsPerTimeSlot>();

        totals = new Totals();

        totalsPerTable = new HashMap<>();
        initTotalsPerTable(deltaTables);
    }

    private void initTotalsPerTable(List<String> tables) {

        for (String table : tables) {
            totalsPerTable.put(table, new RowTotals());
        }
    }

    private void initTimebucket(int currentTimeSeconds) {

        synchronized (criticalSection) {
            // this can be called by multiple threads for the same bucket, so some
            // protection is needed to make sure that bucket is initialized only once

            if (replicatorMetrics.containsKey(currentTimeSeconds)) {
                return;
            }

            replicatorMetrics.put(currentTimeSeconds, new TotalsPerTimeSlot(totals));
        }
    }

    // ROWS
    public void incRowsInsertedCounter(String tableName) {
        synchronized (criticalSection)
        {
            if (!totalsPerTable.containsKey(tableName))
            {
                throw createNoSuchElementException(tableName);
            }

            int currentTimeSeconds = getCurrentTimestamp();
            getOrCreateTimeSlotMetrics(currentTimeSeconds).getRowsForInsertProcessed().increment();

            totalsPerTable.get(tableName).getRowsForInsertProcessed().increment();
        }
    }

    private NoSuchElementException createNoSuchElementException(String tableName) {
        return new NoSuchElementException(String.format("There's no table named %s registered with metrics", tableName));
    }

    public void incRowsUpdatedCounter(String tableName) {

        synchronized (criticalSection)
        {
            if (!totalsPerTable.containsKey(tableName))
            {
                throw createNoSuchElementException(tableName);
            }

            int currentTimeSeconds = getCurrentTimestamp();
            getOrCreateTimeSlotMetrics(currentTimeSeconds).getRowsForUpdateProcessed().increment();
            totalsPerTable.get(tableName).getRowsForUpdateProcessed().increment();
        }
    }

    public void incRowsDeletedCounter(String tableName) {
        synchronized (criticalSection)
        {
            if (!totalsPerTable.containsKey(tableName))
            {
                throw createNoSuchElementException(tableName);
            }

            int currentTimeSeconds = getCurrentTimestamp();
            getOrCreateTimeSlotMetrics(currentTimeSeconds).getRowsForDeleteProcessed().increment();

            totalsPerTable.get(tableName).getRowsForDeleteProcessed().increment();
        }
    }

    public void incRowsProcessedCounter(String tableName) {
        synchronized (criticalSection)
        {
            if (!totalsPerTable.containsKey(tableName))
            {
                throw createNoSuchElementException(tableName);
            }

            int currentTimeSeconds = getCurrentTimestamp();
            getOrCreateTimeSlotMetrics(currentTimeSeconds).getTotalRowsProcessed().increment();

            totalsPerTable.get(tableName).getTotalRowsProcessed().increment();
        }
    }

    // EVENTS
    public void incInsertEventCounter() {
        int currentTimeSeconds = getCurrentTimestamp();
        synchronized (criticalSection)
        {
            getOrCreateTimeSlotMetrics(currentTimeSeconds).getInsertEvents().increment();
        }
    }

    private int getCurrentTimestamp()
    {
        // TODO: don't use ints, use the actual date surrogates

        return (int) (System.currentTimeMillis() / 1000L);
    }

    private TotalsPerTimeSlot getOrCreateTimeSlotMetrics(int timestamp)
    {
        if (!replicatorMetrics.containsKey(timestamp))
        {
            initTimebucket(timestamp);
        }

        return this.replicatorMetrics.get(timestamp);
    }

    public void incUpdateEventCounter() {
        int currentTimeSeconds = getCurrentTimestamp();

        synchronized (criticalSection)
        {
            getOrCreateTimeSlotMetrics(currentTimeSeconds).getUpdateEvents().increment();
        }
    }

    public void incDeleteEventCounter() {
        int currentTimeSeconds = getCurrentTimestamp();

        synchronized (criticalSection)
        {
            getOrCreateTimeSlotMetrics(currentTimeSeconds).getDeleteEvents().increment();
        }
    }

    public void incCommitQueryCounter() {
        int currentTimeSeconds = getCurrentTimestamp();

        synchronized (criticalSection)
        {
            getOrCreateTimeSlotMetrics(currentTimeSeconds).getCommitCounter().increment();
        }
    }

    public void incXIDCounter() {
        int currentTimeSeconds = getCurrentTimestamp();

        synchronized (criticalSection)
        {
            getOrCreateTimeSlotMetrics(currentTimeSeconds).getXidCounter().increment();
        }
    }

    public void incEventsReceivedCounter() {
        int currentTimeSeconds = getCurrentTimestamp();

        synchronized (criticalSection)
        {
            getOrCreateTimeSlotMetrics(currentTimeSeconds).getEventsReceived().increment();
        }
    }

    public void incEventsProcessedCounter() {
        int currentTimeSeconds = getCurrentTimestamp();

        synchronized (criticalSection)
        {
            getOrCreateTimeSlotMetrics(currentTimeSeconds).getEventsProcessed().increment();
        }
    }

    public void incEventsSkippedCounter() {
        int currentTimeSeconds = getCurrentTimestamp();

        synchronized (criticalSection)
        {
            getOrCreateTimeSlotMetrics(currentTimeSeconds).getEventsSkipped().increment();
        }
    }

    public void incHeartBeatCounter() {
        int currentTimeSeconds = getCurrentTimestamp();

        synchronized (criticalSection)
        {
            getOrCreateTimeSlotMetrics(currentTimeSeconds).getHeartBeatCounter().increment();
        }
    }

    public void incApplierTasksSubmittedCounter() {
        int currentTimeSeconds = getCurrentTimestamp();

        synchronized (criticalSection)
        {
            getOrCreateTimeSlotMetrics(currentTimeSeconds).getApplierTasksSubmitted().increment();
        }
    }

    public void incApplierTasksSucceededCounter() {
        int currentTimeSeconds = getCurrentTimestamp();

        synchronized (criticalSection)
        {
            getOrCreateTimeSlotMetrics(currentTimeSeconds).getApplierTasksSucceeded().increment();
        }
    }

    public void incApplierTasksFailedCounter() {
        int currentTimeSeconds = getCurrentTimestamp();

        synchronized (criticalSection)
        {
            getOrCreateTimeSlotMetrics(currentTimeSeconds).getApplierTasksFailed().increment();
        }
    }

    public void incApplierTasksInProgressCounter() {
        int currentTimeSeconds = getCurrentTimestamp();

        synchronized (criticalSection)
        {
            getOrCreateTimeSlotMetrics(currentTimeSeconds).getApplierTasksInProgress().increment();
        }
    }

    // SET:
    public void setReplicatorReplicationDelay(Long replicatorReplicationDelay) {
        int currentTimeSeconds = getCurrentTimestamp();

        synchronized (criticalSection)
        {
            getOrCreateTimeSlotMetrics(currentTimeSeconds).getReplicationDelayMilliseconds().setValue(BigInteger.valueOf(replicatorReplicationDelay));
        }
    }

    // ADD:
    public void incCurrentTimeBucketForRowOpsSuccessfullyCommittedToHBase(Long delta) {
        int currentTimeSeconds = getCurrentTimestamp();

        synchronized (criticalSection)
        {
            getOrCreateTimeSlotMetrics(currentTimeSeconds).getHbaseRowsAffected().incrementBy(delta);
        }
    }


    // TOTALS:

    public void incTotalRowOpsSuccessfullyCommitedToHBase(Long delta, String tableName) {
        synchronized (criticalSection) {
            if (!totalsPerTable.containsKey(tableName)) {
                throw createNoSuchElementException(tableName);
            }

            int currentTimeSeconds = getCurrentTimestamp();
            getOrCreateTimeSlotMetrics(currentTimeSeconds).getTotalHbaseRowsAffected().incrementBy(delta);

            totalsPerTable.get(tableName).getTotalHbaseRowsAffected().incrementBy(delta);
        }
    }

    public void setTaskQueueSize(Long newValue) {

        synchronized (criticalSection)
        {
            int currentTimeSeconds = getCurrentTimestamp();
            getOrCreateTimeSlotMetrics(currentTimeSeconds).getTaskQueueSize().setValue(BigInteger.valueOf(newValue));
        }
    }

    public void removeBucketStats(int timestamp)
    {
        synchronized (criticalSection)
        {
            replicatorMetrics.remove(timestamp);
        }
    }

    public Map<String, RowTotals> getTotalsPerTableSnapshot() {
        HashMap<String, RowTotals> copy = new HashMap<>();

        synchronized (criticalSection)
        {
            for (String tableName : totalsPerTable.keySet()) {

                copy.put(tableName, totalsPerTable.get(tableName).copy());
            }

            return Collections.unmodifiableMap(copy);
        }
    }

    public Map<Integer, TotalsPerTimeSlot> getMetricsSnapshot() {

        HashMap<Integer, TotalsPerTimeSlot> copy = new HashMap<>();

        synchronized (criticalSection)
        {
            for (int timestamp : replicatorMetrics.keySet()) {

                copy.put(timestamp, replicatorMetrics.get(timestamp).copy());
            }

            return Collections.unmodifiableMap(copy);
        }
    }

    public Totals getTotalsSnapshot(){
        synchronized (criticalSection)
        {
            // TODO: this should be readonly
            return totals.copy();
        }
    }
}