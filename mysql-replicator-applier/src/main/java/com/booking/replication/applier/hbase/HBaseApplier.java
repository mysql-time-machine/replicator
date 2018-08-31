package com.booking.replication.applier.hbase;

import com.booking.replication.applier.Applier;
import com.booking.replication.applier.hbase.schema.HBaseSchemaManager;
import com.booking.replication.applier.hbase.writer.HBaseApplierWriter;
import com.booking.replication.applier.hbase.writer.HBaseTimeMachineWriter;
import com.booking.replication.augmenter.model.event.AugmentedEvent;

import com.booking.replication.augmenter.model.event.AugmentedEventType;

import com.booking.replication.augmenter.model.schema.SchemaSnapshot;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.Collection;
import java.util.Map;

import static java.util.stream.Collectors.toList;

public class HBaseApplier implements Applier {

    private static final Logger LOG = LogManager.getLogger(com.booking.replication.applier.hbase.HBaseApplier.class);

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private int FLUSH_BUFFER_SIZE       = 5;
    private int BUFFER_FLUSH_TIME_LIMIT = 10;

    private HBaseSchemaManager hbaseSchemaManager;
    private HBaseApplierWriter hBaseApplierWriter;

    private Map<String, Object> configuration;

    org.apache.hadoop.conf.Configuration hbaseConfig;

    public interface Configuration {
        String HBASE_ZOOKEEPER_QUORUM   = "applier.hbase.zookeeper.quorum";
        String REPLICATED_SCHEMA_NAME   = "applier.hbase.sourcedb.name";
        String TARGET_NAMESPACE         = "applier.hbase.destination.namespace";
        String SCHEMA_HISTORY_NAMESPACE = "applier.hbase.schema.history.namespace";
        String INITIAL_SNAPSHOT_MODE    = "applier.hbase.initial.snapshot";
        String HBASE_USE_SNAPPY         = "applier.hbase.snappy";
        String DRYRUN                   = "applier.hbase.dryrun";
    }

    @SuppressWarnings("unused")
    public HBaseApplier(Map<String, Object> configuration) {

        this.configuration = configuration;

        hbaseConfig = getHBaseConfig(configuration);

        try {

            hbaseSchemaManager = new HBaseSchemaManager(configuration);
            LOG.info("Created HBaseSchemaManager.");

            hBaseApplierWriter = new HBaseTimeMachineWriter(hbaseConfig, configuration);
            LOG.info("Created HBaseApplierWriter.");

        } catch (IOException | NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
    }

    private  org.apache.hadoop.conf.Configuration getHBaseConfig(Map<String, Object> configuration) {
        // TODO: read BigTable specifics from configuration & setup hbaseConfig accordingly
        return HBaseConfiguration.create();
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
     * Coordinator that rows are markedForCommit. In case of small transactions, applier
     * will internally buffer them and apply will return false until buffer is
     * large enough. Once buffer is large enough it will flush the buffer and
     * return true (on success).
     * <p>
     * In short:
     * - return false means that list of rows is buffered
     * - return true means that the list of rows and/or buffered data is markedForCommit.
     * It also means that the safe checkpoint can be advanced forward.
     * - IOException means that all write attempts have failed. This shuts down the whole pipeline.
     * <p>
     * In addition:
     * - In case that flush() fails, the retry logic needs to be implemented
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

//        for (AugmentedEvent ev : events) {
//            try {
//                HBaseApplier.LOG.info(HBaseApplier.MAPPER.writeValueAsString(ev));
//            } catch (JsonProcessingException e) {
//                e.printStackTrace();
//            }
//        }

        try {

            long now = Instant.now().toEpochMilli();

            if (now - hBaseApplierWriter.getBufferClearTime() > BUFFER_FLUSH_TIME_LIMIT &&
                    hBaseApplierWriter.getBufferSize() > 0) {
                LOG.info("Buffer time limit reached - flushing buffer");
                boolean s = hBaseApplierWriter.flush();
                if (s) {
                    return true; // <- markedForCommit, will advance safe checkpoint
                } else {
                    throw new IOException("Failed to write buffer to HBase");
                }
            }

            for (AugmentedEvent ev : events) {
                if (ev.getOptionalPayload() != null) {
                    LOG.info("AugmentedEvent contains optionalPayload");
                    SchemaSnapshot schemaSnapshot = ((SchemaSnapshot)ev.getOptionalPayload());
                    hbaseSchemaManager.writeSchemaSnapshot(schemaSnapshot, this.configuration);
                    LOG.debug(HBaseApplier.MAPPER.writeValueAsString(schemaSnapshot.getSchemaAfter().getTableSchemaCache()));
                }
            }

            events = events.stream().filter(
                    ev ->   (ev.getHeader().getEventType() == AugmentedEventType.WRITE_ROWS)
                             ||
                            (ev.getHeader().getEventType() == AugmentedEventType.UPDATE_ROWS)
                             ||
                            (ev.getHeader().getEventType() == AugmentedEventType.DELETE_ROWS)
            ).collect(toList());

            if ((events.size() >= FLUSH_BUFFER_SIZE) || hBaseApplierWriter.getBufferSize() >= FLUSH_BUFFER_SIZE) {
                hBaseApplierWriter.buffer(events);
                boolean s = hBaseApplierWriter.flush();
                if (s) {
                    return true; // <- markedForCommit, will advance safe checkpoint
                } else {
                    throw new IOException("Failed to write buffer to HBase");
                }
            } else {
                hBaseApplierWriter.buffer(events);
                return false; // buffered
            }
        } catch (Exception e) {
            // TODO:
            //      make this nicer -> apply should return Option<Boolean> and the
            //      caller can handle the empty case explicitly, instead of this hacky
            //      convert of checked into unchecked exception.
            throw new RuntimeException("Failed to write to HBase, all retries exhausted", e);
        }
    }
}
