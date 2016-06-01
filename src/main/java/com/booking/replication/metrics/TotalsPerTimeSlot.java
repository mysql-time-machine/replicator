package com.booking.replication.metrics;

/**
 * Created by mdutikov on 5/27/2016.
 */
public class TotalsPerTimeSlot extends Totals {

    protected Metric replicationDelayMilliseconds;
    protected Metric taskQueueSize;

    public TotalsPerTimeSlot(
            Totals overallTotals) {
        this(
                new CounterWithDependency(overallTotals.getEventsReceived()),
                new CounterWithDependency(overallTotals.getEventsSkipped()),
                new CounterWithDependency(overallTotals.getEventsProcessed()),
                new CounterWithDependency(overallTotals.getInsertEvents()),
                new CounterWithDependency(overallTotals.getUpdateEvents()),
                new CounterWithDependency(overallTotals.getDeleteEvents()),
                new CounterWithDependency(overallTotals.getCommitCounter()),
                new CounterWithDependency(overallTotals.getXidCounter()),
                new CounterWithDependency(overallTotals.getTotalHbaseRowsAffected()),
                new CounterWithDependency(overallTotals.getRowsForDeleteProcessed()),
                new CounterWithDependency(overallTotals.getRowsForInsertProcessed()),
                new CounterWithDependency(overallTotals.getHeartBeatCounter()),
                new CounterWithDependency(overallTotals.getRowsForUpdateProcessed()),
                new CounterWithDependency(overallTotals.getHbaseRowsAffected()),
                new CounterWithDependency(overallTotals.getApplierTasksSubmitted()),
                new CounterWithDependency(overallTotals.getApplierTasksInProgress()),
                new CounterWithDependency(overallTotals.getApplierTasksSucceeded()),
                new CounterWithDependency(overallTotals.getApplierTasksFailed()),
                new Metric("TASK_QUEUE_SIZE"),
                new Metric("REPLICATION_DELAY_MS"));
    }

    public TotalsPerTimeSlot(ICounter eventsReceived,
                  ICounter eventsSkipped,
                  ICounter eventsProcessed,
                  ICounter insertEvents,
                  ICounter updateEvents,
                  ICounter deleteEvents,
                  ICounter commitCounter,
                  ICounter xidCounter,
                  ICounter rowOperationsSuccessfullyCommitted,
                  ICounter rowsForDeleteProcessed,
                  ICounter rowsForInsertProcessed,
                  ICounter heartBeatCounter,
                  ICounter rowsForUpdateProcessed,
                  ICounter rowsProcessed,
                  ICounter applierTasksSubmitted,
                  ICounter applierTasksInProgress,
                  ICounter applierTasksSucceeded,
                  ICounter applierTasksFailed,
                             Metric taskQueueSize,
                             Metric replicationDelayMilliseconds)
    {
        super(eventsReceived, eventsSkipped, eventsProcessed, insertEvents, updateEvents, deleteEvents,
                commitCounter, xidCounter, rowOperationsSuccessfullyCommitted, rowsForDeleteProcessed,
                rowsForInsertProcessed, heartBeatCounter, rowsForUpdateProcessed, rowsProcessed,
                applierTasksSubmitted, applierTasksInProgress, applierTasksSucceeded, applierTasksFailed);

        this.taskQueueSize = taskQueueSize;
        this.replicationDelayMilliseconds = replicationDelayMilliseconds;
    }

    public Metric getTaskQueueSize() {
        return taskQueueSize;
    }

    public Metric getReplicationDelayMilliseconds() {
        return replicationDelayMilliseconds;
    }

    public TotalsPerTimeSlot copy()
    {
        return new TotalsPerTimeSlot(
                getEventsReceived().copy(),
                getEventsSkipped().copy(),
                getEventsProcessed().copy(),
                getInsertEvents().copy(),
                getUpdateEvents().copy(),
                getDeleteEvents().copy(), getCommitCounter().copy(), getXidCounter().copy(),
                getTotalHbaseRowsAffected().copy(), getRowsForDeleteProcessed().copy(),
                getRowsForInsertProcessed().copy(), getHeartBeatCounter().copy(), getRowsForUpdateProcessed().copy(),
                getHbaseRowsAffected().copy(), getApplierTasksSubmitted().copy(), getApplierTasksInProgress().copy(),
                getApplierTasksSucceeded().copy(), getApplierTasksFailed().copy(),taskQueueSize.copy(), replicationDelayMilliseconds.copy());
    }
}
