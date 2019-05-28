package com.booking.replication.applier.hbase.writer;

import com.booking.replication.applier.hbase.HBaseApplier;
import com.booking.replication.applier.hbase.mutation.HBaseApplierMutationGenerator;
import com.booking.replication.applier.hbase.schema.HBaseRowKeyMapper;
import com.booking.replication.applier.hbase.schema.HBaseSchemaManager;
import com.booking.replication.applier.hbase.time.RowTimestampOrganizer;
import com.booking.replication.augmenter.util.AugmentedEventRowExtractor;
import com.booking.replication.augmenter.model.event.AugmentedEvent;
import com.booking.replication.augmenter.model.row.AugmentedRow;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.booking.replication.commons.metrics.Metrics;

public class HBaseTimeMachineWriter implements HBaseApplierWriter {

        private static final Logger LOG = LogManager.getLogger(HBaseTimeMachineWriter.class);
        private final Metrics<?> metrics;

        private final int FLUSH_RETRY_LIMIT = 30;
        private ThreadLocal<Long> bufferClearTime = ThreadLocal.withInitial(() -> Instant.now().toEpochMilli());
        private final HBaseSchemaManager hbaseSchemaManager;
        private RowTimestampOrganizer timestampOrganizer;
        HBaseApplierMutationGenerator mutationGenerator;
        private final boolean dryRun;

        Connection connection;
        Admin admin;

        ConcurrentHashMap<Long,Map<String,Collection<AugmentedEvent>>> buffered;

        public HBaseTimeMachineWriter(Configuration hbaseConfig,
                                      HBaseSchemaManager hbaseSchemaManager,
                                      Map<String, Object> configuration)
                throws IOException, NoSuchAlgorithmException {

            this.metrics = Metrics.getInstance(configuration);

            this.hbaseSchemaManager = hbaseSchemaManager;

            this.metrics.getRegistry()
                    .counter("hbase.applier.connection.attempt").inc(1L);
            connection = ConnectionFactory.createConnection(hbaseConfig);
            this.metrics.getRegistry()
                    .counter("hbase.applier.connection.success").inc(1L);

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
                Boolean s = flushThreadBuffer(threadID);

                if (s) {
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
                    .histogram("hbase.thread_" + threadID + "_applier.writer.buffer.thread_" + threadID + ".nr_transactions_buffered")
                    .update( buffered.get(threadID).size() );

            List<HBaseApplierMutationGenerator.PutMutation> mutations = new ArrayList<>();

            for ( String transactionUUID: buffered.get(threadID).keySet() ) {
                for (AugmentedEvent event : buffered.get(threadID).get(transactionUUID)) {
                    List<AugmentedRow> augmentedRows = AugmentedEventRowExtractor.extractAugmentedRows(event);

                    String augmentedRowsTableName = extractTableName(augmentedRows);

                    hbaseSchemaManager.createHBaseTableIfNotExists(augmentedRowsTableName);

                    this.metrics.getRegistry()
                            .counter("hbase.thread_" + threadID + ".applier.writer.rows.received").inc(augmentedRows.size());

                    if (timestampOrganizer != null) {
                        timestampOrganizer.organizeTimestamps(augmentedRows, augmentedRowsTableName, threadID, transactionUUID);
                    }

                    List<HBaseApplierMutationGenerator.PutMutation> eventMutations = augmentedRows.stream()
                            .flatMap(
                                    row -> Stream.of(mutationGenerator.getPutForMirroredTable(row))
                            )
                            .collect(Collectors.toList());
                    mutations.addAll(eventMutations);

                    this.metrics.getRegistry()
                            .counter("hbase.thread_" + threadID + ".applier.writer.mutations_generated").inc(eventMutations.size());

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
            for (Map.Entry<String, List<HBaseApplierMutationGenerator.PutMutation>> entry : mutationsByTable.entrySet()){

                String tableName = entry.getKey();
                List<HBaseApplierMutationGenerator.PutMutation> tableMutations = entry.getValue();

                List<Put> putList = tableMutations.stream().map(
                        mutation -> mutation.getPut()
                ).collect(Collectors.toList());

                long tBegin = System.currentTimeMillis();

                long nrMutations = putList.size();

                Table table = connection.getTable(TableName.valueOf(tableName));
                table.put(putList);
                table.close();

                long tEnd = System.currentTimeMillis();
                long latency = tEnd - tBegin;

                this.metrics
                        .getRegistry()
                        .histogram("hbase.thread_" + threadID + ".applier.writer.put.latency")
                        .update(latency);

                this.metrics
                        .getRegistry()
                        .histogram("hbase.thread_" + threadID + ".applier.writer.put.nr-mutations")
                        .update(nrMutations);

                // TODO: send sample to validator
            }
        }
}
