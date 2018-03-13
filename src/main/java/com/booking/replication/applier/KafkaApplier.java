package com.booking.replication.applier;

import com.booking.replication.Configuration;
import com.booking.replication.Metrics;
import com.booking.replication.applier.kafka.KafkaMessageBufferException;
import com.booking.replication.applier.kafka.RowListMessage;
import com.booking.replication.augmenter.AugmentedRow;
import com.booking.replication.augmenter.AugmentedRowsEvent;
import com.booking.replication.augmenter.AugmentedSchemaChangeEvent;
import com.booking.replication.checkpoints.PseudoGTIDCheckpoint;
import com.booking.replication.pipeline.CurrentTransaction;
import com.booking.replication.pipeline.PipelineOrchestrator;
import com.booking.replication.schema.exception.TableMapException;
import com.booking.replication.util.CaseInsensitiveMap;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import com.google.code.or.binlog.BinlogEventV4;
import com.google.code.or.binlog.impl.event.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.booking.replication.applier.kafka.Util.getHashCode_HashCustomColumn;
import static com.booking.replication.applier.kafka.Util.getHashCode_HashPrimaryKeyValuesMethod;
import static com.codahale.metrics.MetricRegistry.name;

public class KafkaApplier implements Applier {

    // how many rows go into one message
    private static final int MESSAGE_BATCH_SIZE = 1; // 10;

    private static boolean DRY_RUN;

    private static long totalRowsCounter = 0;
    private static long totalOutliersCounter = 0;

    private KafkaProducer<String, String> producer;
    private KafkaConsumer<String, String> consumer;

    private static List<String> fixedListOfIncludedTables;
    private static List<String> excludeTablePatterns;

    private static final Map<String,Boolean> wantedTables = new CaseInsensitiveMap<>();

    // We need to make sure that all rows from one table end up on the same
    // partition. That is why we have a separate buffer for each partition, so
    // during buffering the right buffer is chosen.
    private HashMap<Integer,RowListMessage> partitionCurrentMessageBuffer = new HashMap<>();

    private String topicName;
    private final boolean apply_begin_event;
    private final boolean apply_commit_event;
    private final boolean apply_uuid;
    private final boolean apply_xid;
    private AtomicBoolean exceptionFlag = new AtomicBoolean(false);

    private final Meter meterForMessagesPushedToKafka;
    private static final Counter exception_counter = Metrics.registry.counter(name("Kafka", "exceptionCounter"));
    private static final Counter outlier_counter = Metrics.registry.counter(name("Kafka", "outliersCounter"));
    private static final Timer closingTimer = Metrics.registry.timer(name("Kafka", "producerCloseTimer"));

    private static final HashMap<Integer, RowListMessage> partitionLastBufferedRow = new HashMap<>();
    private static final HashMap<Integer, RowListMessage> partitionLastCommittedMessage = new HashMap<>();

    private int numberOfPartition;
    private String brokerAddress;
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaApplier.class);
    private String rowLastPositionID = "";
    private String messageLastPositionID = "";

    private int paritioningMethod;
    private HashMap<String, String> partitionColumns;

    private PseudoGTIDCheckpoint lastCheckpointCommittedByApplier;

    private static Properties getProducerProperties(String broker) {
        // Below is the new version of producer configuration
        Properties prop = new Properties();
        prop.put("bootstrap.servers", broker);
        prop.put("acks", "all"); // Default 1
        prop.put("retries", 30); // Default value: 0
        prop.put("batch.size", 16384); // Default value: 16384
        // prop.put("linger.ms", 20); // Default 0, Artificial delay
        prop.put("buffer.memory", 33554432); // Default value: 33554432
        prop.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        prop.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        prop.put("metric.reporters", "com.booking.replication.applier.KafkaMetricsCollector");
        prop.put("request.timeout.ms", 100000);
        return prop;
    }

    private static Properties getConsumerProperties(String broker) {
        // Consumer configuration
        Properties prop = new Properties();
        prop.put("bootstrap.servers", broker);
        prop.put("group.id", "getLastCommittedMessages");
        prop.put("auto.offset.reset", "latest");
        prop.put("enable.auto.commit", "false");
        prop.put("auto.commit.interval.ms", "1000");
        prop.put("session.timeout.ms", "30000");
        prop.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        prop.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        return prop;
    }

    public KafkaApplier(Configuration configuration, Meter meterForMessagesPushedToKafka) {

        DRY_RUN = configuration.isDryRunMode();

        fixedListOfIncludedTables = configuration.getKafkaTableList();
        excludeTablePatterns      = configuration.getKafkaExcludeTableList();
        topicName                 = configuration.getKafkaTopicName();
        brokerAddress             = configuration.getKafkaBrokerAddress();
        apply_begin_event         = configuration.isKafkaApplyBeginEvent();
        apply_commit_event        = configuration.isKafkaApplyCommitEvent();
        apply_uuid                = configuration.getAugmenterApplyUuid();
        apply_xid                 = configuration.getAugmenterApplyXid();
        paritioningMethod         = configuration.getKafkaPartitioningMethod();
        partitionColumns          = configuration.getKafkaPartitionColumns();

        this.meterForMessagesPushedToKafka = meterForMessagesPushedToKafka;

        if (!DRY_RUN) {

            producer = new KafkaProducer<>(getProducerProperties(brokerAddress));

            numberOfPartition = producer.partitionsFor(topicName).size();

            consumer = new KafkaConsumer<>(getConsumerProperties(brokerAddress));

            LOGGER.info("Start to fetch last positions");

            // Fetch last committed messages on each partition in order to prevent duplicate messages
            loadLastMessagePositionForEachPartition();

            LOGGER.info("Size of partitionLastCommittedMessage: " + partitionLastCommittedMessage.size());

            for (Integer i : partitionLastCommittedMessage.keySet()) {
                LOGGER.info(
                        "{ partition: "                    + i.toString() + "} -> " +
                        "{ lastCommittedMessageUniqueID: " + partitionLastCommittedMessage.get(i) + " }"
                );
            }
        }
    }

    @Override
    public SupportedAppliers.ApplierName getApplierName() {
        return SupportedAppliers.ApplierName.KafkaApplier;
    }

    @Override
    public void applyAugmentedRowsEvent(AugmentedRowsEvent augmentedDataEvent, CurrentTransaction currentTransaction) {

        for (AugmentedRow augmentedRow : augmentedDataEvent.getSingleRowEvents()) {

            if (exceptionFlag.get()) throw new RuntimeException("Producer has problem with sending messages, could be a connection issue");

            if (augmentedRow.getTableName() == null) throw new RuntimeException("tableName does not exist");

            if (!tableIsWanted(augmentedRow.getTableName())) {
                totalOutliersCounter++;
                outlier_counter.inc();
                return;
            }

            totalRowsCounter++;

            updateRowLastPositionID(augmentedRow.getRowBinlogPositionID());

            pushToBuffer(getPartitionNum(augmentedRow), augmentedRow);
        }
    }

    @Override
    public void applyBeginQueryEvent(QueryEvent event, CurrentTransaction currentTransaction) {
        if (!apply_begin_event) {
            LOGGER.debug("Dropping BEGIN event because applyBeginEvent is off");
            return;
        }
        LOGGER.debug("Applying BEGIN event");
        if (exceptionFlag.get()) throw new RuntimeException("Producer has problem with sending messages, could be a connection issue");

        AugmentedRow augmentedRow;
        try {
            augmentedRow = new AugmentedRow(event.getBinlogFilename(), 0, null, null, "BEGIN", event.getHeader(), currentTransaction.getUuid(), currentTransaction.getXid(), apply_uuid, apply_xid);
        } catch (TableMapException e) {
            throw new RuntimeException("Failed to create AugmentedRow for BEGIN event: ", e);
        }

        updateRowLastPositionID(augmentedRow.getRowBinlogPositionID());
        pushToBuffer(getPartitionNum(augmentedRow), augmentedRow);
    }

    @Override
    public void applyCommitQueryEvent(QueryEvent event, CurrentTransaction currentTransaction) {
        if (!apply_commit_event) {
            LOGGER.debug("Dropping COMMIT event because applyCommitEvent is off");
            return;
        }
        LOGGER.debug("Applying COMMIT event");
        if (exceptionFlag.get()) throw new RuntimeException("Producer has problem with sending messages, could be a connection issue");

        AugmentedRow augmentedRow;
        try {
            augmentedRow = new AugmentedRow(event.getBinlogFilename(), 0, null, null, "COMMIT", event.getHeader(), currentTransaction.getUuid(), currentTransaction.getXid(), apply_uuid, apply_xid);
        } catch (TableMapException e) {
            throw new RuntimeException("Failed to create AugmentedRow for COMMIT event: ", e);
        }

        updateRowLastPositionID(augmentedRow.getRowBinlogPositionID());
        pushToBuffer(getPartitionNum(augmentedRow), augmentedRow);
    }

    @Override
    public void applyXidEvent(XidEvent event, CurrentTransaction currentTransaction) {
        if (!apply_commit_event) {
            LOGGER.debug("Dropping XID event because applyBeginEvent is off");
            return;
        }
        LOGGER.debug("Applying XID event");
        if (exceptionFlag.get()) throw new RuntimeException("Producer has problem with sending messages, could be a connection issue");

        AugmentedRow augmentedRow;
        try {
            augmentedRow = new AugmentedRow(event.getBinlogFilename(), 0, null, null, "XID", event.getHeader(), currentTransaction.getUuid(), currentTransaction.getXid(), apply_uuid, apply_xid);
        } catch (TableMapException e) {
            throw new RuntimeException("Failed to create AugmentedRow for XID event: ", e);
        }

        updateRowLastPositionID(augmentedRow.getRowBinlogPositionID());
        pushToBuffer(getPartitionNum(augmentedRow), augmentedRow);
    }

    @Override
    public void applyRotateEvent(RotateEvent event) {

    }

    @Override
    public void applyAugmentedSchemaChangeEvent(AugmentedSchemaChangeEvent augmentedSchemaChangeEvent, PipelineOrchestrator caller) {

    }

    @Override
    public void applyFormatDescriptionEvent(FormatDescriptionEvent event) {

    }

    @Override
    public void applyTableMapEvent(TableMapEvent event) {

    }

    private void loadLastMessagePositionForEachPartition() {
        // Method to fetch the last committed message in each partition of each topic.
        final int RetriesLimit = 100;
        final int POLL_TIME_OUT = 1000;
        ConsumerRecord<String, String> lastMessage;
        ConsumerRecords<String, String> messages;

        // loop partitions
        for (PartitionInfo pi: producer.partitionsFor(topicName)) {

            TopicPartition partition = new TopicPartition(topicName, pi.partition());
            consumer.assign(Collections.singletonList(partition));
            LOGGER.info("Position: " + String.valueOf(consumer.position(partition)));
            long endPosition = consumer.position(partition);

            // There is an edge case here. With a brand new partition, consumer position is equal to 0
            if (endPosition > 0) {
                LOGGER.info(String.format("Consumer seek to position minus one, current position %d", endPosition));
                consumer.seek(partition, endPosition - 1);
                if (consumer.position(partition) != endPosition - 1) {
                    LOGGER.error("Error seek position minus one");
                }
                int retries = 0;
                while (!partitionLastCommittedMessage.containsKey(pi.partition()) && retries < RetriesLimit) {
                    // We have rewinded the position one element back from the last one, so the list of messages
                    // returned by poll method will only contain one message
                    messages = consumer.poll(POLL_TIME_OUT);
                    if (!messages.isEmpty()) {

                        lastMessage = messages.iterator().next();

                        // ------------------------------------------------------------------------------
                        // Update last message position cache:
                        // if this message ID is not cached in the last committed message cache, or if
                        // there is a cached message ID that is older than the last message, update cache
                        // with the last message ID for this parition
                        String lastMessageBinlogPositionID = lastMessage.key();
                        RowListMessage lastMessageDecoded = RowListMessage.fromJSON(lastMessage.value());

                        if (!partitionLastCommittedMessage.containsKey(pi.partition()) ||
                            partitionLastCommittedMessage.get(pi.partition()).getLastRowBinlogPositionID().compareTo(lastMessageBinlogPositionID) < 0) {
                            partitionLastCommittedMessage.put(pi.partition(), lastMessageDecoded);
                        }

                        // ------------------------------------------------------------------------------
                        // Update row position cache:
                        //
                        // now we need to get the last row id that was in that last message and update last
                        // row position cache (that is needed to compare with rows arrving from producer)
                        // in order to avoid duplicate rows being pushed to kafka
                        String lastRowBinlogPositionID = lastMessageDecoded.getLastRowBinlogPositionID();
                        String lastPseudoGTID = lastMessageDecoded.getLastPseudoGTID();

                        if (!partitionLastBufferedRow.containsKey(pi.partition()) ||
                            partitionLastBufferedRow.get(pi.partition()).getLastRowBinlogPositionID().compareTo(lastRowBinlogPositionID) < 0) {
                            partitionLastBufferedRow.put(pi.partition(), lastMessageDecoded);
                        } else if (!partitionLastBufferedRow.containsKey(pi.partition()) ||
                           (partitionLastBufferedRow.get(pi.partition()).getLastPseudoGTID() != null && lastPseudoGTID != null &&
                            partitionLastBufferedRow.get(pi.partition()).getLastPseudoGTID().compareTo(lastPseudoGTID) < 0)) {
                            partitionLastBufferedRow.put(pi.partition(), lastMessageDecoded);
                        }
                    }
                    retries++;
                }
                if (!partitionLastCommittedMessage.containsKey(pi.partition())) {
                    LOGGER.error("Poll failed, probably the messages got purged!");
                    // throw new RuntimeException("Poll failed, probably the messages got purged!");
                }
            }
        }
    }

    private boolean tableIsWanted(String tableName) {
        if (wantedTables.containsKey(tableName)) {
            return wantedTables.get(tableName);
        } else {
            // First check if the exclude pattern is specified. If
            // there is no exclude pattern, then check for the fixed
            // list of tables. If the exclude pattern is present it
            // overrides the fixed list of tables.
            if (excludeTablePatterns != null) {
                for (String excludePattern : excludeTablePatterns) {
                    Pattern compiledExcludePattern = Pattern.compile(excludePattern, Pattern.CASE_INSENSITIVE);
                    Matcher matcher = compiledExcludePattern.matcher(tableName);
                    if (matcher.find()) {
                        wantedTables.put(tableName,false);
                        return false;
                    }
                }
                // still here, meaning table should not be excluded
                wantedTables.put(tableName,true);
                return true;
            } else {
                // using fixed list of tables since the exclude pattern is
                // not specified
                for (String includedTable : fixedListOfIncludedTables) {
                    Pattern compiledIncludePattern = Pattern.compile(includedTable, Pattern.CASE_INSENSITIVE);
                    Matcher matcher = compiledIncludePattern.matcher(tableName);
                    if (matcher.find()) {
                        wantedTables.put(tableName,true);
                        return true;
                    }
                }
                // table is not in the included list, so should not be replicated
                wantedTables.put(tableName,false);
                return false;
            }
        }
    }

    private int getHashcodeForRow(AugmentedRow row) {
        int hashCode;

        String eventType = row.getEventType();
        String tableName = row.getTableName();
        List<String> pkColumns = row.getPrimaryKeyColumns();
        Map<String, Map<String, String>> eventColumns = row.getEventColumns();

        // The partitioning configuration doesn't apply for those events
        if (eventType.equals("BEGIN")
                || eventType.equals("COMMIT")
                || eventType.equals("XID")
                ) {
            hashCode = row.hashCode();
        } else {
            switch (this.paritioningMethod) {
                case Configuration.PARTITIONING_METHOD_HASH_ROW:
                    hashCode = row.hashCode();
                    break;
                case Configuration.PARTITIONING_METHOD_HASH_TABLE_NAME:
                    hashCode = tableName.hashCode();
                    break;
                case Configuration.PARTITIONING_METHOD_HASH_PRIMARY_COLUMN:
                    hashCode = getHashCode_HashPrimaryKeyValuesMethod(
                         eventType,pkColumns, eventColumns
                    );
                    break;
                case Configuration.PARTITIONING_METHOD_HASH_CUSTOM_COLUMN:
                    hashCode = getHashCode_HashCustomColumn(
                        eventType, tableName, eventColumns, partitionColumns
                    );
                    break;
                default:
                    hashCode = tableName.hashCode();
                    break;
            }
        }
        return hashCode;
    }

     private int getPartitionNum(AugmentedRow row) {
        if (DRY_RUN) {
            return 0;
        }
        int hashCode = this.getHashcodeForRow(row);
        return (hashCode % numberOfPartition + numberOfPartition) % numberOfPartition;
    }

    /**
     * Push to Kafka broker if one of the following is true:
     *     1. there are no rows on current partition
     *     2. If current message unique ID is greater than the last committed
     *        message unique ID
     * */
    private void pushToBuffer(int partitionNum, AugmentedRow augmentedRow) {

        String rowBinlogPositionID = augmentedRow.getRowBinlogPositionID();

        if (isAfterLastRow(partitionNum, rowBinlogPositionID)) {
            // if buffer is not initialized for partition, do init
            if (partitionCurrentMessageBuffer.get(partitionNum) == null) {
                List<AugmentedRow> rowsBucket = new ArrayList<>();
                rowsBucket.add(augmentedRow);
                partitionCurrentMessageBuffer.put(partitionNum, new RowListMessage(MESSAGE_BATCH_SIZE, rowsBucket, this.lastCheckpointCommittedByApplier != null?this.lastCheckpointCommittedByApplier.getPseudoGTID():null));
            } else {
                // if buffer is full do:
                //      (close) -> (send message) -> (create new buffer - sets current row as the first in the buffer)
                // else:
                //      (add current row to the buffer)
                if (partitionCurrentMessageBuffer.get(partitionNum).isFull()) {

                    // 1. close buffer
                    partitionCurrentMessageBuffer.get(partitionNum).closeMessageBuffer();

                    // 2. send message
                    sendMessage(partitionNum);

                    // 3. open new buffer with current row as buffer-start-row
                    List<AugmentedRow> rowsBucket = new ArrayList<>();
                    rowsBucket.add(augmentedRow);
                    partitionCurrentMessageBuffer.put(partitionNum, new RowListMessage(MESSAGE_BATCH_SIZE, rowsBucket, this.lastCheckpointCommittedByApplier != null?this.lastCheckpointCommittedByApplier.getPseudoGTID():null));

                } else {
                    // buffer row to current buffer
                    try {
                        partitionCurrentMessageBuffer.get(partitionNum).addRowToMessage(augmentedRow);
                    } catch (KafkaMessageBufferException ke) {
                        LOGGER.error("Trying to write to a closed buffer. This should never happen. Exiting...");
                        System.exit(-1);
                    }
                }
            }
            meterForMessagesPushedToKafka.mark();
        } else {
            LOGGER.debug("Row for partitionNum " + partitionNum + " skipped: " + augmentedRow);
        }
    }

    private boolean isAfterLastRow(int partitionNum, String rowBinlogPositionID) {

        return
                // if no messages in partition then there is no last row,
                // so current row is the latest for that partition
                (!partitionLastBufferedRow.containsKey(partitionNum))
                ||
                // temporarily we still use binlog positions, but this is deprecated and
                // in the non-beta release it will be removed in favour of pseudoGTIDs only.
                (rowBinlogPositionID.compareTo(partitionLastBufferedRow.get(partitionNum).getLastRowBinlogPositionID()) > 0)
                ||
                // pseudoGTID checkpoints are ascending strings.
                (
                    this.lastCheckpointCommittedByApplier != null
                    &&
                    this
                        .lastCheckpointCommittedByApplier
                        .getPseudoGTID()
                        .compareTo(
                            partitionLastBufferedRow
                                .get(partitionNum)
                                .getLastPseudoGTID()
                        ) > 0
                );
    }

    private void updateRowLastPositionID(String rowBinlogPositionID) {
        // Row binlog position id. Position inside a begin event always 0 because there's only one "row"
        if (rowBinlogPositionID.compareTo(rowLastPositionID) <= 0) {
            throw new RuntimeException(
                    String.format("Something wrong with the row position. This should never happen. Current position: %s. Previous: %s", rowBinlogPositionID, rowLastPositionID));
        }
        rowLastPositionID = rowBinlogPositionID;
    }

    private void sendMessage(int partitionNum) {

        RowListMessage rowListMessage = partitionCurrentMessageBuffer.get(partitionNum);
        String jsonMessage = rowListMessage.toJSON();

        if (DRY_RUN) {
            System.out.println(jsonMessage);
            return;
        }

        ProducerRecord<String, String> message = new ProducerRecord<>(
                topicName,
                partitionNum,
                rowListMessage.getMessageBinlogPositionID(),
                jsonMessage);

        producer.send(
                message,
                (recordMetadata, sendException) -> {
            if (sendException != null) {
                LOGGER.error("Error producing to Kafka broker", sendException);
                exceptionFlag.set(true);
                exception_counter.inc();
            }
        });
    }

    @Override
    public void forceFlush() {

        LOGGER.debug("Kafka Applier force flush");

        // flush partition buffers
        for (int partitionNum : partitionCurrentMessageBuffer.keySet()) {

            LOGGER.debug("will force flush partition " + partitionNum);

            if ( partitionCurrentMessageBuffer.get(partitionNum) != null) {

                LOGGER.debug("content to force flush: " + partitionCurrentMessageBuffer.get(partitionNum).toJSON());

                partitionCurrentMessageBuffer.get(partitionNum).closeMessageBuffer();

                sendMessage(partitionNum);

                // open new buffer
                partitionCurrentMessageBuffer.put(partitionNum, null);

            } else {
                LOGGER.debug("nothing to flush for partition " + partitionNum);
            }
        }

        final Timer.Context context = closingTimer.time();

        // Producer close does the waiting, see documentation.
        producer.close();
        context.stop();

        producer = new KafkaProducer<>(getProducerProperties(brokerAddress));
        LOGGER.info("A new producer has been created");
    }

    @Override
    public void waitUntilAllRowsAreCommitted() {

        final Timer.Context context = closingTimer.time();

        // Producer close does the waiting, see documentation.
        producer.close();

        context.stop();

        producer = new KafkaProducer<>(getProducerProperties(brokerAddress));

        LOGGER.info("A new producer has been created");
    }

    @Override
    public PseudoGTIDCheckpoint getLastCommittedPseudGTIDCheckPoint() {
        return null;
    }

    @Override
    public void applyPseudoGTIDEvent(PseudoGTIDCheckpoint pseudoGTIDCheckPoint) {
        waitUntilAllRowsAreCommitted();
        this.lastCheckpointCommittedByApplier = pseudoGTIDCheckPoint;
    }
}
