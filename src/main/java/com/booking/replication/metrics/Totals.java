package com.booking.replication.metrics;

/**
 * Created by mdutikov on 5/27/2016.
 */
public class Totals extends RowTotals {
    protected ICounter eventsReceived;
    protected ICounter eventsSkipped;
    protected ICounter eventsProcessed;
    protected ICounter insertEvents;
    protected ICounter updateEvents;
    protected ICounter deleteEvents;
    protected ICounter commitCounter;
    protected ICounter xidCounter;

    protected ICounter heartBeatCounter;

    protected ICounter applierTasksSubmitted;
    protected ICounter applierTasksInProgress;
    protected ICounter applierTasksSucceeded;
    protected ICounter applierTasksFailed;

    public Totals()
    {
        this(
                new Counter("EVENTS_RECEIVED"),
                new Counter("EVENTS_SKIPPED"),
                new Counter("EVENTS_PROCESSED"),
                new Counter("INSERT_EVENTS_COUNT"),
                new Counter("UPDATE_EVENTS_COUNT"),
                new Counter("DELETE_EVENTS_COUNT"),
                new Counter("COMMIT_QUERIES_COUNT"),
                new Counter("XID_COUNT"),
                new Counter("HBASE_ROWS_AFFECTED"),
                new Counter("ROWS_FOR_DELETE_PROCESSED"),
                new Counter("TOTAL_ROWS_FOR_INSERT_PROCESSED"),
                new Counter("HEART_BEAT_COUNTER"),
                new Counter("ROWS_FOR_UPDATE_PROCESSED"),
                new Counter("TOTAL_ROWS_PROCESSED"),
                new Counter("TOTAL_APPLIER_TASKS_SUBMITTED"),
                new Counter("TOTAL_APPLIER_TASKS_IN_PROGRESS"),
                new Counter("TOTAL_APPLIER_TASKS_SUCCEEDED"),
                new Counter("TOTAL_APPLIER_TASKS_FAILED"));
    }

    public Totals(ICounter eventsReceived,
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
                  ICounter applierTasksFailed)
    {
        // TODO: arg checks

        super(rowOperationsSuccessfullyCommitted, rowsForDeleteProcessed, rowsForInsertProcessed, rowsForUpdateProcessed, rowsProcessed);

        this.eventsReceived = eventsReceived;
        this.eventsSkipped = eventsSkipped;
        this.eventsProcessed = eventsProcessed;
        this.insertEvents = insertEvents;
        this.updateEvents = updateEvents;
        this.deleteEvents = deleteEvents;
        this.commitCounter = commitCounter;
        this.xidCounter = xidCounter;
        this.heartBeatCounter = heartBeatCounter;
        this.applierTasksSubmitted = applierTasksSubmitted;
        this.applierTasksInProgress = applierTasksInProgress;
        this.applierTasksSucceeded = applierTasksSucceeded;
        this.applierTasksFailed = applierTasksFailed;
    }

    public ICounter getEventsReceived() {
        return eventsReceived;
    }

    public ICounter getEventsSkipped() {
        return eventsSkipped;
    }

    public ICounter getEventsProcessed() {
        return eventsProcessed;
    }

    public ICounter getInsertEvents() {
        return insertEvents;
    }

    public ICounter getUpdateEvents() {
        return updateEvents;
    }

    public ICounter getDeleteEvents() {
        return deleteEvents;
    }

    public ICounter getCommitCounter() {
        return commitCounter;
    }

    public ICounter getXidCounter() {
        return xidCounter;
    }

    public ICounter getHeartBeatCounter() {
        return heartBeatCounter;
    }

    public ICounter getApplierTasksSubmitted() {
        return applierTasksSubmitted;
    }

    public ICounter getApplierTasksInProgress() {
        return applierTasksInProgress;
    }

    public ICounter getApplierTasksSucceeded() {
        return applierTasksSucceeded;
    }

    public ICounter getApplierTasksFailed() {
        return applierTasksFailed;
    }

    public Totals copy()
    {
        return new Totals(
                eventsReceived.copy(),
                eventsSkipped.copy(),
                eventsProcessed.copy(),
                insertEvents.copy(),
                updateEvents.copy(),
                deleteEvents.copy(),
                commitCounter.copy(),
                xidCounter.copy(),
                getTotalHbaseRowsAffected().copy(),
                getRowsForDeleteProcessed().copy(),
                getRowsForInsertProcessed().copy(),
                heartBeatCounter.copy(),
                getRowsForUpdateProcessed().copy(),
                getHbaseRowsAffected().copy(),
                applierTasksSubmitted.copy(),
                applierTasksInProgress.copy(),
                applierTasksSucceeded.copy(),
                applierTasksFailed.copy()
                );
    }
}
