package com.booking.replication.applier.hbase;

import com.booking.replication.applier.Applier;
import com.booking.replication.applier.hbase.schema.HBaseRowKeyMapper;
import com.booking.replication.applier.hbase.schema.HBaseSchemaManager;
import com.booking.replication.applier.hbase.schema.SchemaTransitionException;
import com.booking.replication.applier.hbase.writer.HBaseApplierWriter;
import com.booking.replication.applier.hbase.writer.HBaseTimeMachineWriter;
import com.booking.replication.augmenter.model.event.*;
import com.booking.replication.augmenter.model.row.AugmentedRow;
import com.booking.replication.augmenter.model.schema.SchemaSnapshot;
import com.booking.replication.augmenter.util.AugmentedEventRowExtractor;
import com.booking.replication.commons.metrics.Metrics;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toList;

public class HBaseApplier implements Applier {

    private static final Logger LOG = LogManager.getLogger(HBaseApplier.class);

    private static final int DEFAULT_FLUSH_BUFFER_SIZE        = 1000;
    private static final int DEFAULT_BUFFER_FLUSH_TIME_LIMIT  = 30;

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private final Metrics<?> metrics;

    private final int FLUSH_BUFFER_SIZE;
    private final int BUFFER_FLUSH_TIME_LIMIT;

    private final boolean dryRun;

    private HBaseSchemaManager hbaseSchemaManager;
    private HBaseApplierWriter hBaseApplierWriter;

    private Map<String, Object> configuration;
    private final StorageConfig storageConfig;

    org.apache.hadoop.conf.Configuration hbaseConfig;

    public interface Configuration {
        String HBASE_ZOOKEEPER_QUORUM   = "applier.hbase.zookeeper.quorum";
        String REPLICATED_SCHEMA_NAME   = "applier.hbase.sourcedb.name";
        String TARGET_NAMESPACE         = "applier.hbase.destination.namespace";
        String SCHEMA_HISTORY_NAMESPACE = "applier.hbase.schema.history.namespace";
        String INITIAL_SNAPSHOT_MODE    = "applier.hbase.initial.snapshot";
        String HBASE_USE_SNAPPY         = "applier.hbase.snappy";
        String DRYRUN                   = "applier.hbase.dryrun";
        String PAYLOAD_TABLE_NAME       = "applier.hbase.payload.table.name";
        String FLUSH_BUFFER_SIZE        = "applier.hbase.buffer.size";
        String BUFFER_FLUSH_TIME_LIMIT  = "applier.hbase.buffer.time.limit";
    }

    @SuppressWarnings("unused")
    public HBaseApplier(Map<String, Object> configuration) {

        this.configuration = configuration;

        this.dryRun = (boolean) configuration.get(Configuration.DRYRUN);

        if (configuration.containsKey(Configuration.FLUSH_BUFFER_SIZE)) {
            FLUSH_BUFFER_SIZE = (int) configuration.get(Configuration.FLUSH_BUFFER_SIZE);
        }else {
            FLUSH_BUFFER_SIZE = DEFAULT_FLUSH_BUFFER_SIZE;
        }

        LOG.info("HBase FLUSH_BUFFER_SIZE set to " + FLUSH_BUFFER_SIZE);

        if (configuration.containsKey(Configuration.BUFFER_FLUSH_TIME_LIMIT)) {
            BUFFER_FLUSH_TIME_LIMIT= (int) configuration.get(Configuration.BUFFER_FLUSH_TIME_LIMIT);
        }else {
            BUFFER_FLUSH_TIME_LIMIT = DEFAULT_BUFFER_FLUSH_TIME_LIMIT;
        }

        LOG.info("HBase BUFFER_FLUSH_TIME_LIMIT to " + BUFFER_FLUSH_TIME_LIMIT);

        this.metrics = Metrics.getInstance(configuration);
        this.storageConfig = StorageConfig.build(configuration);

        hbaseConfig = storageConfig.getConfig();

        try {
            hbaseSchemaManager = new HBaseSchemaManager(configuration);
            LOG.info("Created HBaseSchemaManager.");

            hBaseApplierWriter = new HBaseTimeMachineWriter(hbaseConfig, hbaseSchemaManager,configuration);
            LOG.info("Created HBaseApplierWriter.");

        } catch (IOException | NoSuchAlgorithmException e) {
            LOG.error(e.getMessage(), e);
        }

        if (hbaseSchemaManager == null) {
            throw new RuntimeException("Failed to initialize HBaseSchemaManager");
        }

        if (hBaseApplierWriter == null) {
            throw new RuntimeException("Failed to initialize HBaseTimeMachineWriter");
        }

    }

    /**
     * Logic of Operation:
     * <p>
     * The Streams implementation runs multiple threads and will partition the
     * tasks by transaction id.
     * <p>
     * The Augmenter will send grouped transactions to the Applier.apply(). In case of
     * large transactions it will send lists of 1000 rows.
     * <p>
     * The apply() method returns Boolean. If it returns true, this is a signal for
     * Coordinator that rows are committed. In case of small transactions, applier
     * will internally buffer them and apply will return false until buffer is
     * large enough. Once buffer is large enough it will flushThreadBuffer the buffer and
     * return true (on success).
     * <p>
     * In short:
     * - return false means that list of rows is buffered
     * - return true means that the list of rows and/or buffered data is markedForCommit.
     * It also means that the safe checkpoint can be advanced forward.
     * - IOException means that all write attempts have failed. This shuts down the whole pipeline.
     * <p>
     * In addition:
     * - In case that flushThreadBuffer() fails, the retry logic needs to be implemented
     * in the ApplierWriter. The Streams implementation manages threads and
     * groups rows by transactions. Once the transaction batch has been sent
     * to the ApplierWriter it is the responsibility of the
     * ApplierWriter implementation to implement write-retry logic.
     * <p>
     * <p>
     * True/False from apply is only relevant for storing the checkpoint. However, in
     * case of error, apply should throw and Exception and the whole pipeline will shutdown.
     */
    @Override
    public Boolean apply(Collection<AugmentedEvent> events) {
        Long threadID = Thread.currentThread().getId();

        this.metrics
                .getRegistry()
                .histogram("hbase.thread_" + threadID + ".applier.events.apply.batchsize")
                .update(events.size());

        for (AugmentedEvent event : events) {
            this.metrics.getRegistry()
                    .counter("hbase.thread_" + threadID + ".applier.events.seen").inc(1L);
        }

        if (dryRun) {
            for (AugmentedEvent ev: events) {
                List<AugmentedRow> augmentedRows = AugmentedEventRowExtractor.extractAugmentedRows(ev);
                augmentedRows.stream().forEach(row ->
                    System.out.println(
                        row.getEventType() + ":" +
                            "\ttable => " + row.getTableName() +
                            "\tkey => " + HBaseRowKeyMapper.getSaltedHBaseRowKey(row) +
                            "\tcommitTimestamp => " + row.getCommitTimestamp() +
                            "\ttransactionCounter => " + row.getMicrosecondTransactionOffset() / 100 +
                            "\tmicrosecondTimestamp => " + row.getRowMicrosecondTimestamp())
                    );
            }
            return true;
        }
        checkIfBufferExpired();

        try {
            doSchemaLog(events);
        } catch (IOException e) {
            LOG.error(e.getMessage(), e);
        } catch (SchemaTransitionException e) {
           throw  new RuntimeException("SchemaTransitionException",e);
        }

        List<AugmentedEvent> dataEvents = extractDataEventsOnly(events);

        List<String> transactionUUIDs = getTransactionUUIDs(events);


        if (transactionUUIDs.size() == 1) {
            String transactionUUID = transactionUUIDs.get(0);
            if ((dataEvents.size() >= FLUSH_BUFFER_SIZE) || hBaseApplierWriter.getThreadBufferSize(threadID) >= FLUSH_BUFFER_SIZE) {
                hBaseApplierWriter.buffer(threadID, transactionUUID, dataEvents);
                this.metrics.getRegistry()
                        .counter("hbase.thread_" + threadID + ".applier.buffer.buffered").inc(1L);
                this.metrics.getRegistry()
                        .counter("hbase.thread_" + threadID + ".applier.buffer.flush.attempt").inc(1L);
                boolean s = hBaseApplierWriter.flushThreadBuffer(threadID);

                if (s) {
                    this.metrics.getRegistry()
                            .counter("thread_" + threadID + ".hbase_applier.buffer.flush.success").inc(1L);
                    return true; // <- committed, will advance safe checkpoint
                } else {
                    this.metrics.getRegistry()
                            .counter("thread_" + threadID + ".hbase_applier.buffer.flush.failure").inc(1L);
                    throw new RuntimeException("Failed to write buffer to HBase");
                }
            } else {
                this.metrics.getRegistry()
                        .counter("thread_" + threadID + ".hbase_applier.buffer.buffered").inc(1L);
                hBaseApplierWriter.buffer(threadID, transactionUUID, dataEvents);
                return false; // buffered
            }
        } else if (transactionUUIDs.size() > 1) {

            // multiple transactions in one list
            for (String transactionUUID : transactionUUIDs) {
                hBaseApplierWriter.buffer(threadID, transactionUUID, dataEvents);
                this.metrics.getRegistry()
                        .counter("thread_" + threadID + ".hbase_applier.buffer.buffered").inc(1L);
            }
            this.metrics.getRegistry()
                    .counter("thread_" + threadID + ".hbase_applier.buffer.flush.force.attempt").inc(1L);
            forceFlush();
            this.metrics.getRegistry()
                    .counter("thread_" + threadID + ".hbase_applier.buffer.flush.force.success").inc(1L);
            return true;
        } else {
            this.metrics.getRegistry()
                    .counter("thread_" + threadID + ".hbase_applier.events.empty").inc(1L);
            return false; // treat empty transaction as buffered
        }
    }

    private List<String> getTransactionUUIDs(Collection<AugmentedEvent> events) {
        return events
                .stream()
                .filter(e -> e.getHeader().getEventTransaction() != null)
                .map(e -> e.getHeader().getEventTransaction().getIdentifier())
                .distinct()
                .collect(Collectors.toList());
    }

    private void doSchemaLog(Collection<AugmentedEvent> events) throws IOException, SchemaTransitionException {
        // schema snapshot (DLL log)
        for (AugmentedEvent ev : events) {
            if (ev.getOptionalPayload() != null) {

                SchemaSnapshot schemaSnapshot = ((SchemaSnapshot)ev.getOptionalPayload());

                hbaseSchemaManager.writeSchemaSnapshot(schemaSnapshot, this.configuration);

                LOG.debug(HBaseApplier.MAPPER.writeValueAsString(schemaSnapshot.getSchemaAfter().getTableSchemaCache()));
            }
        }
    }

    private List<AugmentedEvent> extractDataEventsOnly(Collection<AugmentedEvent> events) {
        // data only (events are already grouped by transaction when passed to apply, so no need to process begin/commit)
        return events.stream().filter(
                ev ->   (ev.getHeader().getEventType() == AugmentedEventType.WRITE_ROWS)
                         ||
                        (ev.getHeader().getEventType() == AugmentedEventType.UPDATE_ROWS)
                         ||
                        (ev.getHeader().getEventType() == AugmentedEventType.DELETE_ROWS)
        ).collect(toList());
    }

    private void checkIfBufferExpired() {
        long now = Instant.now().toEpochMilli();
        if(now - hBaseApplierWriter.getThreadLastFlushTime() > BUFFER_FLUSH_TIME_LIMIT) {
            forceFlush();
        }
    }

    @Override
    public boolean forceFlush() {
        boolean s;
        try {
            s = hBaseApplierWriter.forceFlushThreadBuffer( Thread.currentThread().getId() );
        } catch (IOException e) {
            throw new RuntimeException("forceFlushThreadBuffer() failed");
        }
        return s;
    }

    public boolean forceFlushAll() {
        boolean s;
        try {
            s = hBaseApplierWriter.forceFlushAllThreadBuffers();
        } catch (IOException e) {
            throw new RuntimeException("forceFlushThreadBuffer() failed");
        }
        return s;
    }

}
