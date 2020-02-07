package com.booking.replication.applier.hbase.writer;

import com.booking.replication.applier.hbase.HBaseApplier;
import com.booking.replication.applier.hbase.mutation.HBaseApplierMutationGenerator;
import com.booking.replication.applier.hbase.schema.HBaseSchemaManager;
import com.booking.replication.applier.hbase.time.RowTimestampOrganizer;
import com.booking.replication.applier.validation.ValidationService;
import com.booking.replication.augmenter.model.event.AugmentedEvent;
import com.booking.replication.augmenter.model.row.AugmentedRow;
import com.booking.replication.augmenter.util.AugmentedEventRowExtractor;
import com.booking.replication.commons.metrics.Metrics;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class HBaseTimeMachineWriter implements HBaseApplierWriter {

    private static final Logger LOG = LogManager.getLogger(HBaseTimeMachineWriter.class);
    private final Metrics<?> metrics;

    private final int FLUSH_RETRY_LIMIT = 30;

    private ThreadLocal<Long> bufferClearTime = ThreadLocal.withInitial(() -> Instant.now().toEpochMilli());
    private final HBaseSchemaManager hbaseSchemaManager;
    private RowTimestampOrganizer timestampOrganizer;
    HBaseApplierMutationGenerator mutationGenerator;
    private final boolean dryRun;
    private final ValidationService validationService;
    private final String payloadTableName;

    Connection connection;
    Admin admin;

    ConcurrentHashMap<Long, Map<String, Collection<AugmentedEvent>>> buffered;

    public HBaseTimeMachineWriter(Configuration hbaseConfig,
                                  HBaseSchemaManager hbaseSchemaManager,
                                  Map<String, Object> configuration,
                                  ValidationService validationService)
            throws IOException, NoSuchAlgorithmException {

        this.validationService = validationService;

        this.payloadTableName = (String) configuration.get(HBaseApplier.Configuration.PAYLOAD_TABLE_NAME);

        this.metrics = Metrics.getInstance(configuration);

        this.hbaseSchemaManager = hbaseSchemaManager;

        this.metrics.getRegistry()
                .counter("applier.hbase.connection.attempt").inc(1L);
        connection = ConnectionFactory.createConnection(hbaseConfig);
        this.metrics.getRegistry()
                .counter("applier.hbase.connection.success").inc(1L);

        admin = connection.getAdmin();

        buffered = new ConcurrentHashMap<>();

        mutationGenerator = new HBaseApplierMutationGenerator(configuration, metrics);

        timestampOrganizer = new RowTimestampOrganizer(); // <- TODO: if not initial_snapshot_mode()

        dryRun = (boolean) configuration.get(HBaseApplier.Configuration.DRYRUN);
    }

    @Override
    public void buffer(Long threadID, String transactionUUID, Collection<AugmentedEvent> events) {
        if (buffered.get(threadID) == null) {
            buffered.put(threadID, new HashMap<>());
        }

        if ( buffered.get(threadID).get(transactionUUID) == null ) {
            buffered.get(threadID).put(transactionUUID, new ArrayList<>());
        }

        for (AugmentedEvent event : events) {
            buffered.get(threadID).get(transactionUUID).add(event);
        }
    }

    @Override
    public long getThreadLastFlushTime() {
        return bufferClearTime.get();
    }

    @Override
    public int getThreadBufferSize(Long threadID) {
        if (buffered.get(threadID) == null) {
            return 0;
        } else {
            return buffered.get(threadID).size();
        }
    }

    @Override
    public boolean forceFlushAllThreadBuffers() throws IOException {
        Boolean result = true;
        for ( Long id : buffered.keySet() ) {
            result = result && flushThreadBuffer(id);

            if ( result == false ) {
                throw new IOException("Failed to forceFlush buffer for thread " + id + " to HBase");
            }
        }

        return true;
    }

    @Override
    public boolean forceFlushThreadBuffer(Long threadID) throws IOException {
        if ( buffered.contains(threadID) ) {
            Boolean isFlushThreadBufferSuccess = flushThreadBuffer(threadID);

            if (isFlushThreadBufferSuccess) {
                return true; // <- markedForCommit, will advance safe checkpoint
            } else {
                throw new RuntimeException("Failed to write buffer to HBase");
            }
        } else {
            return true;
        }
    }

    @Override
    public boolean flushThreadBuffer(Long threadID) {
        if (buffered.get(threadID) == null) {
            throw new RuntimeException("Called flushThreadBuffer for thread with empty buffer");
        }
        boolean result = false;
        try {
            result = flushThreadBufferWithRetry(threadID); // false means all retries have failed

            if ( result == true ) {
                buffered.remove(threadID);
            } else {
                throw new RuntimeException("Exceeded retry limit when writing to HBase/BigTable");
            }

            bufferClearTime.set( Instant.now().toEpochMilli() );
        } catch (InterruptedException e) {
            LOG.error(e.getMessage(), e);
        }
        return result;
    }

    private Boolean flushThreadBufferWithRetry(Long threadID) throws InterruptedException {

        int counter = FLUSH_RETRY_LIMIT;

        while (counter > 0) {
            counter--;

            try {
                writeToHBase(threadID);
                return true;
            } catch (IOException e) {
                LOG.warn("Failed to write to HBase.", e);
            }
            Thread.sleep(1000); // TODO: exponential backoff
        }
        return false;
    }

    private String extractTableName(List<AugmentedRow> augmentedRows) {
        List<String> tables = augmentedRows.stream().map(ar -> ar.getTableName()).collect(Collectors.toList());
        if (tables != null) {
            Set unique = new HashSet(tables);
            if (unique.size() > 1) {
                throw new RuntimeException("More than one table in binlog event not allowed!");
            }
            String tableName = tables.get(0);
            return  tableName;
        } else {
            throw new RuntimeException("No table found in AugmentedRow list!");
        }
    }

    private void writeToHBase(Long threadID) throws IOException {

        this.metrics.getRegistry()
                .histogram("applier.hbase.writer.buffer.thread_" + threadID + ".nr_transactions_buffered")
                .update( buffered.get(threadID).size() );

        List<HBaseApplierMutationGenerator.PutMutation> mutations = new ArrayList<>();

        for ( String transactionUUID: buffered.get(threadID).keySet() ) {
            for (AugmentedEvent event : buffered.get(threadID).get(transactionUUID)) {
                List<AugmentedRow> augmentedRows = AugmentedEventRowExtractor.extractAugmentedRows(event);

                String augmentedRowsTableName = extractTableName(augmentedRows);

                hbaseSchemaManager.createHBaseTableIfNotExists(augmentedRowsTableName);

                this.metrics.getRegistry()
                        .counter("applier.hbase.writer.thread_" + threadID + ".rows.received").inc(augmentedRows.size());

                if (timestampOrganizer != null) {
                    timestampOrganizer.organizeTimestamps(augmentedRows, augmentedRowsTableName, threadID, transactionUUID);
                }

                List<HBaseApplierMutationGenerator.PutMutation> eventMutations = augmentedRows.stream()
                        .flatMap(row -> Stream.of(mutationGenerator.getPutForMirroredTable(row)))
                        .collect(Collectors.toList());
                mutations.addAll(eventMutations);

                this.metrics.getRegistry()
                        .counter("applier.hbase.writer.thread_" + threadID + ".mutations_generated").inc(eventMutations.size());

            }
        }

        // group by table
        Map<String, List<HBaseApplierMutationGenerator.PutMutation>> mutationsByTable =
                mutations
                        .stream()
                        .collect(
                                Collectors.groupingBy( mutation -> mutation.getTable() )
                        );

        // write to hbase
        for (Map.Entry<String, List<HBaseApplierMutationGenerator.PutMutation>> entry : mutationsByTable.entrySet()) {

            String tableName = entry.getKey();
            List<HBaseApplierMutationGenerator.PutMutation> tableMutations = entry.getValue();

            List<Put> putList = tableMutations
                    .stream()
                    .map(mutation -> mutation.getPut())
                    .collect(Collectors.toList());

            long timeBegin = System.currentTimeMillis();

            long nrMutations = putList.size();

            BufferedMutator mutator = connection.getBufferedMutator(TableName.valueOf(tableName));
            mutator.mutate(putList);
            mutator.flush();
            mutator.close();
//            Table table = connection.getTable(TableName.valueOf(tableName));
//            table.put(putList);
//            table.close();

            for (HBaseApplierMutationGenerator.PutMutation mutation : tableMutations){
                if (validationService != null && !tableName.equalsIgnoreCase(payloadTableName)) validationService.registerValidationTask(mutation.getTransactionUUID(), mutation.getSourceRowUri(), mutation.getTargetRowUri());
            }

            long timeEnd = System.currentTimeMillis();
            long latency = timeEnd - timeBegin;

            this.metrics
                    .getRegistry()
                    .histogram("applier.writer.hbase.thread_" + threadID + ".put.latency")
                    .update(latency);

            this.metrics
                    .getRegistry()
                    .histogram("applier.writer.hbase.thread_" + threadID + ".put.nr-mutations")
                    .update(nrMutations);

        }
    }
}
