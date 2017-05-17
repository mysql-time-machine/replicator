package com.booking.replication.pipeline;

import com.booking.replication.Configuration;
import com.booking.replication.Constants;
import com.booking.replication.Metrics;

import com.booking.replication.binlog.event.*;
import com.booking.replication.replicant.ReplicantPool;
import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.BinaryLogClient.EventListener;
import com.github.shyiko.mysql.binlog.event.Event;

import com.google.code.or.OpenReplicator;
import com.google.code.or.binlog.BinlogEventV4;
import com.google.code.or.binlog.BinlogEventListener;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;
import com.google.code.or.common.util.MySQLConstants;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static com.codahale.metrics.MetricRegistry.name;

/**
 * Simple wrapper for Open Replicator. Writes events to blocking queue.
 */
public class BinlogEventProducer {

    // queue is private, but it will reference the same queue
    // as the consumer object

    private final BlockingQueue<RawBinlogEvent> rawBinlogEventQueue;

    private PipelinePosition pipelinePosition;

    private Object           binlogEventParserProvider;

    private final int              BINLOG_EVENT_PARSER_PROVIDER_CODE;

    private final Configuration    configuration;
    private final ReplicantPool replicantPool;

    private final int serverId = (new Random().nextInt() >>> 1) | (1 << 30); // a large positive random integer;

    private long opCounter = 0;
    private long backPressureSleep = 0;

    private static final Logger LOGGER = LoggerFactory.getLogger(BinlogEventProducer.class);

    private static final Meter producedEvents = Metrics.registry.meter(name("events", "eventsProduced"));

    /**
     * Set up and manage the binlog provider instance instance.
     * @param rawBinlogEventQueue
     * @param pipelinePosition  Binlog position information
     * @param configuration     Replicator configuration
     * @param binlogParserProviderCode
     */
    public BinlogEventProducer(

        LinkedBlockingQueue<RawBinlogEvent> rawBinlogEventQueue,
        PipelinePosition                    pipelinePosition,
        Configuration                       configuration,
        ReplicantPool                       replicantPool,
        int                                 binlogParserProviderCode

    ) throws Exception {

        this.rawBinlogEventQueue = rawBinlogEventQueue;
        this.pipelinePosition = pipelinePosition;
        this.configuration = configuration;
        this.replicantPool = replicantPool;

        this.BINLOG_EVENT_PARSER_PROVIDER_CODE = binlogParserProviderCode;

        Random random = new Random();

        int serverId = (random.nextInt() >>> 1) | (1 << 30); // a large positive random integer

        binlogEventParserProvider =
                BinlogEventParserProviderFactory.getBinlogEventParserProvider(
                        serverId,
                        BINLOG_EVENT_PARSER_PROVIDER_CODE,
                        configuration,
                        pipelinePosition
                );

        Metrics.registry.register(name("events", "producerBackPressureSleep"),
                (Gauge<Long>) () -> backPressureSleep);
    }

    private void startBinaryLogClient(BinaryLogClient binaryLogClient) throws IOException, Exception {

        binaryLogClient.registerEventListener(new EventListener() {

            @Override
            public void onEvent(Event event) {
                producedEvents.mark();

                // This call is blocking the writes from server side. If time goes above
                // net_write_timeout (which defaults to 60s) server will drop connection.
                //      => Use back pressure to regulate the write rate to the queue.
                if (isRunning()) {
                    boolean eventQueued = false;
                    while (!eventQueued) { // blocking block
                        try {
                            backPressureSleep();
                            boolean added = false;
                            try {
                                RawBinlogEvent rawBinlogEvent;
                                switch (event.getHeader().getEventType()) {
                                    case QUERY:
                                        rawBinlogEvent = new RawBinlogEventQuery(event);
                                        break;
                                    case WRITE_ROWS:
                                        rawBinlogEvent = new RawBinlogEventWriteRows(event);
                                        break;
                                    case UPDATE_ROWS:
                                        rawBinlogEvent = new RawBinlogEventUpdateRows(event);
                                        break;
                                    case DELETE_ROWS:
                                        rawBinlogEvent = new RawBinlogEventDeleteRows(event);
                                        break;
                                    case TABLE_MAP:
                                        rawBinlogEvent = new RawBinlogEventTableMap(event);
                                        break;
                                    case FORMAT_DESCRIPTION:
                                        rawBinlogEvent = new RawBinlogEventFormatDescription(event);
                                        break;
                                    case ROTATE:
                                        rawBinlogEvent = new RawBinlogEventRotate(event);
                                        break;
                                    case STOP:
                                        rawBinlogEvent = new RawBinlogEventStop(event);
                                        break;
                                    case XID:
                                        rawBinlogEvent = new RawBinlogEventXid(event);
                                        break;
                                    default:
                                        rawBinlogEvent = new RawBinlogEvent(event);
                                        LOGGER.warn("Unsupported event type: " + event.getHeader().getEventType());
                                        break;
                                }
                                // there is no binlog file name in the binlog connector event, so need to
                                // inject the binlog file name of the last red event (which is maintained
                                // by the binlog client)
                                rawBinlogEvent.setBinlogFilename(binaryLogClient.getBinlogFilename());

//                                if (rawBinlogEvent.getEventType() == RawEventType.WRITE_ROWS_EVENT) {
//                                    LOGGER.info("=====>" + event.getData().toString());
//                                    RawBinlogEventWriteRows rows = (RawBinlogEventWriteRows) rawBinlogEvent;
//                                    for (Row row : rows.getExtractedRows()) {
//                                        LOGGER.info("------> " + row.toString());
//                                    }
//                                }

                                added = rawBinlogEventQueue.offer(rawBinlogEvent, 100, TimeUnit.MILLISECONDS);
                            } catch (Exception e) {
                                LOGGER.error("rawBinlogEventsQueue.offer failed.", e);
                            }
                            if (added) {
                                opCounter++;
                                eventQueued = true;
                                if (opCounter % 10000 == 0) {
                                    LOGGER.info("Producer reporting queue size => " + rawBinlogEventQueue.size());
                                }
                            } else {
                                LOGGER.error("queue.offer timed out. Will sleep for 100ms and try again");
                                Thread.sleep(100);
                            }
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
        });
        LOGGER.info("starting BinaryLogClient from: { binlog-file => "
                + binaryLogClient.getBinlogFilename()
                + ", position => "
                + binaryLogClient.getBinlogPosition()
                + " }"
        );
        binaryLogClient.connect(10000);
    }

    /**
     * Start.
     * @throws Exception openReplicator Exception
     */
    public void startOpenReplicator(OpenReplicator openReplicator) throws Exception {

        openReplicator.setBinlogEventListener(

                new BinlogEventListener() {

                      public void onEvents(BinlogEventV4 event) {

                          producedEvents.mark();

                          // This call is blocking the writes from server side. If time goes above
                          // net_write_timeout (which defaults to 60s) server will drop connection.
                          //      => Use back pressure to regulate the write rate to the queue.

                          if (isRunning()) {
                              boolean eventQueued = false;
                              while (!eventQueued) { // blocking block
                                  try {
                                      backPressureSleep();
                                      boolean added = false;
                                      try {
                                          RawBinlogEvent rawBinlogEvent;
                                          switch (event.getHeader().getEventType()) {
                                              // Check for DDL and pGTID:
                                              case MySQLConstants.QUERY_EVENT:
                                                  rawBinlogEvent = new RawBinlogEventQuery(event);
                                                  break;
                                              case MySQLConstants.TABLE_MAP_EVENT:
                                                  rawBinlogEvent = new RawBinlogEventTableMap(event);
                                                  break;
                                              case MySQLConstants.UPDATE_ROWS_EVENT:
                                              case MySQLConstants.UPDATE_ROWS_EVENT_V2:
                                                  rawBinlogEvent = new RawBinlogEventUpdateRows(event);
                                                  break;
                                              case MySQLConstants.WRITE_ROWS_EVENT:
                                              case MySQLConstants.WRITE_ROWS_EVENT_V2:
                                                  rawBinlogEvent = new RawBinlogEventWriteRows(event);
                                                  break;
                                              case MySQLConstants.DELETE_ROWS_EVENT:
                                              case MySQLConstants.DELETE_ROWS_EVENT_V2:
                                                  rawBinlogEvent = new RawBinlogEventDeleteRows(event);
                                                  break;
                                              case MySQLConstants.XID_EVENT:
                                                  rawBinlogEvent = new RawBinlogEventXid(event);
                                                  break;
                                              case MySQLConstants.FORMAT_DESCRIPTION_EVENT:
                                                  rawBinlogEvent = new RawBinlogEventFormatDescription(event);
                                                  break;
                                              case MySQLConstants.ROTATE_EVENT:
                                                  rawBinlogEvent = new RawBinlogEventRotate(event);
                                                  break;
                                              case MySQLConstants.STOP_EVENT:
                                                  rawBinlogEvent = new RawBinlogEventStop(event);
                                                  break;
                                              default:
                                                  rawBinlogEvent = new RawBinlogEvent(event);
                                                  break;
                                          }
                                          added = rawBinlogEventQueue.offer(rawBinlogEvent, 100, TimeUnit.MILLISECONDS);
                                      } catch (Exception e) {
                                          LOGGER.error("rawBinlogEventsQueue.offer failed ", e);
                                      }

                                      if (added) {
                                          opCounter++;
                                          eventQueued = true;
                                          if (opCounter % 100000 == 0) {
                                              LOGGER.info("Producer reporting queue size => " + rawBinlogEventQueue.size());
                                          }
                                      } else {
                                          LOGGER.error("queue.offer timed out. Will sleep for 100ms and try again");
                                          Thread.sleep(100);
                                      }

                                  } catch (InterruptedException e) {
                                      e.printStackTrace();
                                  }
                              }
                          }
                      }
                  });

        LOGGER.info("starting Open Replicator from: { binlog-file => "
                + openReplicator.getBinlogFileName()
                + ", position => "
                + openReplicator.getBinlogPosition()
                + " }"
        );

        openReplicator.start();
    }

    public void clearQueue() {
        LOGGER.debug("Clearing queue");
        rawBinlogEventQueue.clear();
    }

    public void stopAndClearQueue(long timeout, TimeUnit unit) throws Exception {
        stop(timeout, unit);
        clearQueue();
    }

    private void backPressureSleep() {
        int queueSize = rawBinlogEventQueue.size();

        // For an explanation please plug "max(0, 20*(10000/(10000+1-x)-10)) x from 6000 to 10000" into WolframAlpha
        backPressureSleep = Math.max(
            20 * ( Constants.MAX_RAW_QUEUE_SIZE / (Constants.MAX_RAW_QUEUE_SIZE + 1 - queueSize) - 10),
            0
        );

        // Queue size is 9243.(42 period) (where MAX_RAW_QUEUE_SIZE = 10000)
        if (backPressureSleep < 64) {
            return;
        }

        // Queue size is 9953.(380952 period)
        if (backPressureSleep > 4000) {
            LOGGER.warn("Queue is getting big, back pressure is getting high");
        }

        try {
            Thread.sleep(backPressureSleep);
        } catch (InterruptedException e) {
            LOGGER.error("Thread wont sleep");
            e.printStackTrace();
        }
    }

    public void stopOpenReplicator(OpenReplicator openReplicator, long timeout, TimeUnit unit) throws Exception {
        openReplicator.stop(timeout, unit);
    }

    public void stopBinaryLogClient (BinaryLogClient binaryLogClient, long timeout, TimeUnit unit) throws Exception {
        binaryLogClient.disconnect();
    }

    public boolean isRunning() {
        if (BINLOG_EVENT_PARSER_PROVIDER_CODE == BinlogEventParserProviderCode.OR) {
            return ((OpenReplicator)binlogEventParserProvider).isRunning();
        }
        else { // if (BINLOG_EVENT_PARSER_PROVIDER_CODE == BinlogEventParserProviderCode.OR) {
            return  ((BinaryLogClient)binlogEventParserProvider).isConnected();
        }
    }

    public void stop(long timeout, TimeUnit unit) throws Exception {
        if (BINLOG_EVENT_PARSER_PROVIDER_CODE == BinlogEventParserProviderCode.OR) {
            stopOpenReplicator((OpenReplicator)binlogEventParserProvider,  timeout, unit);
        }
        else {
            stopBinaryLogClient((BinaryLogClient)binlogEventParserProvider,timeout, unit);
        }
    }

    public void start() throws Exception {
        if (binlogEventParserProvider instanceof OpenReplicator) {
            startOpenReplicator((OpenReplicator) binlogEventParserProvider);
        }
        else if (binlogEventParserProvider instanceof BinaryLogClient) {
            startBinaryLogClient((BinaryLogClient) binlogEventParserProvider);
        }
        else {
            throw new Exception("Unsupported parser exception");
        }
    }

    public void rewindHead(String binlogFilename, long binlogPosition) throws Exception {

        PipelinePosition newPipelinePosition = new PipelinePosition(
                pipelinePosition.getCurrentReplicantHostName(), // does not change during rewind
                pipelinePosition.getCurrentReplicantServerID(), // does not change during rewind
                binlogFilename,
                binlogPosition,
                pipelinePosition.getLastSafeCheckPointPosition().getBinlogFilename(), // does not change during rewind
                pipelinePosition.getLastSafeCheckPointPosition().getBinlogPosition() // dones not change during rewind
        );

        restartFromPosition(newPipelinePosition);
    }

    private void restartFromPosition(PipelinePosition newPipelinePosition) throws Exception {

        this.stopAndClearQueue(10000, TimeUnit.MILLISECONDS);

        this.pipelinePosition = newPipelinePosition;

        // create new binlog producer from the new pipelinePosition
        this.binlogEventParserProvider =
                BinlogEventParserProviderFactory.getBinlogEventParserProvider(
                        serverId,
                        BINLOG_EVENT_PARSER_PROVIDER_CODE,
                        configuration,
                        pipelinePosition
                );

        this.start();
    }
}
