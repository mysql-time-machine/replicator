package com.booking.replication.applier.hbase.writer;

import com.booking.replication.applier.hbase.mutation.HBaseApplierMutationGenerator;
import com.booking.replication.applier.hbase.schema.HBaseSchemaManager;
import com.booking.replication.applier.hbase.time.RowTimestampOrganizer;
import com.booking.replication.applier.hbase.util.AugmentedEventRowExtractor;
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
import com.booking.replication.commons.metrics.Metrics;
import com.codahale.metrics.MetricRegistry;

public class HBaseTimeMachineWriter implements HBaseApplierWriter {

        private static final Logger LOG = LogManager.getLogger(HBaseTimeMachineWriter.class);
        private final Metrics<?> metrics;

        private final int FLUSH_RETRY_LIMIT = 30;
        private long bufferClearTime = 0L;

        private final String HBASE_COLUMN_DEFAULT_FAMILY_NAME = "d";

        private HBaseSchemaManager hbaseSchemaManager;
        private RowTimestampOrganizer timestampOrganizer;
        HBaseApplierMutationGenerator mutationGenerator;

        Connection connection;
        Admin admin;

        ConcurrentHashMap<String,Collection<AugmentedEvent>> buffered;

        public HBaseTimeMachineWriter(Configuration hbaseConfig,
                                      HBaseSchemaManager hbaseSchemaManager,
                                      Map<String, Object> configuration)
                throws IOException, NoSuchAlgorithmException {

            this.metrics = Metrics.build(configuration);

            this.hbaseSchemaManager = hbaseSchemaManager;

            connection = ConnectionFactory.createConnection(hbaseConfig);

            admin = connection.getAdmin();

            buffered = new ConcurrentHashMap<>();

            mutationGenerator = new HBaseApplierMutationGenerator(configuration, metrics);

            timestampOrganizer = new RowTimestampOrganizer(); // <- TODO: if not initial_snapshot_mode()
        }

        @Override
        public void buffer(String transactionUUID, Collection<AugmentedEvent> events) {
            if (buffered.get(transactionUUID) == null) {
                buffered.put(transactionUUID, new ArrayList<>());
            }
            for (AugmentedEvent event : events) {
                buffered.get(transactionUUID).add(event);
            }
        }

        @Override
        public long getBufferClearTime() {
            return bufferClearTime;
        }


        @Override
        public int getTransactionBufferSize(String transactionUUID) {
            if (buffered.get(transactionUUID) == null) {
                return 0;
            } else {
                return buffered.get(transactionUUID).size();
            }
        }

        @Override
        public boolean forceFlush() throws IOException {

            List<String> transactionsBuffered = buffered.keySet().stream().collect(Collectors.toList());

            Boolean s = transactionsBuffered
                             .stream()
                             .map(tUUID -> flushTransactionBuffer(tUUID))
                             .filter(result -> result == false)
                             .collect(Collectors.toList())
                             .isEmpty();
            if (s) {
                return true; // <- markedForCommit, will advance safe checkpoint
            } else {
                throw new IOException("Failed to write buffer to HBase");
            }
        }

        @Override
        public boolean flushTransactionBuffer(String transactionUUID) {
            if (buffered.get(transactionUUID) == null) {
                throw new RuntimeException("Called flushTransactionBuffer for non existing transaction");
            }
            boolean result = false;
            try {
                result = flushWithRetry(transactionUUID); // false means all retries have failed
                buffered.remove(transactionUUID);

                bufferClearTime = Instant.now().toEpochMilli();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            return result;
        }

        private Boolean flushWithRetry(String transactionUUID) throws InterruptedException {

            int counter = FLUSH_RETRY_LIMIT;

            while (counter > 0) {
                counter--;

                try {
                    writeToHBase(transactionUUID);
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
                String mySqlTableName = tables.get(0);
                return  mySqlTableName;
            } else {
                throw new RuntimeException("No table found in AugmentedRow list!");
            }
        }

        private void writeToHBase(String transactionUUID) throws IOException {

            Collection<AugmentedEvent> events = buffered.get(transactionUUID);
            List<HBaseApplierMutationGenerator.PutMutation> mutations = new ArrayList<>();

            // extract augmented rows
            for (AugmentedEvent event : events) {

                List<AugmentedRow> augmentedRows = AugmentedEventRowExtractor.extractAugmentedRows(event);

                this.metrics.getRegistry()
                        .counter("hbase.applier.rows.received.count").inc(augmentedRows.size());

                if (timestampOrganizer != null) {
                    String mySqlTableName = extractTableName(augmentedRows); // TODO: optimize, can be pre-calculated
                    timestampOrganizer.organizeTimestamps(augmentedRows, mySqlTableName, transactionUUID);
                }

                List<HBaseApplierMutationGenerator.PutMutation> eventMutations = augmentedRows.stream()
                        .flatMap(
                                row -> Stream.of(mutationGenerator.getPutForMirroredTable(row))
                        )
                        .collect(Collectors.toList());
                mutations.addAll(eventMutations);

                this.metrics.getRegistry()
                        .counter("hbase.applier.put.count").inc(eventMutations.size());

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

                Table table = connection.getTable(TableName.valueOf(tableName));
                table.put(putList);
                table.close();
                // TODO: send sample to validator
            }
        }

}
