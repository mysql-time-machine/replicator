package com.booking.replication.pipeline;

import com.booking.replication.Configuration;
import com.booking.replication.applier.Applier;
import com.booking.replication.applier.DummyApplier;
import com.booking.replication.binlog.event.*;
import com.booking.replication.pipeline.event.handler.TransactionSizeLimitException;
import com.booking.replication.replicant.DummyReplicantPool;
import com.booking.replication.replicant.ReplicantPool;
import com.booking.replication.schema.DummyActiveSchemaVersion;
import com.booking.replication.util.Cmd;
import com.booking.replication.util.StartupParameters;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.code.or.binlog.BinlogEventV4;
import com.google.code.or.binlog.impl.event.BinlogEventV4HeaderImpl;
import com.google.code.or.binlog.impl.event.QueryEvent;
import com.google.code.or.binlog.impl.event.WriteRowsEventV2;
import com.google.code.or.binlog.impl.event.XidEvent;
import com.google.code.or.common.glossary.column.StringColumn;
import com.google.code.or.common.util.MySQLConstants;
import joptsimple.OptionSet;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.concurrent.LinkedBlockingQueue;

import static org.junit.Assert.*;


/**
 * Created by edmitriev on 8/2/17.
 */
public class PipelineOrchestratorTest {
    private static String[] args = new String[1];

    private static LinkedBlockingQueue<RawBinlogEvent>
            rawBinlogEventLinkedBlockingQueue = new LinkedBlockingQueue<>();

    private static LinkedBlockingQueue<RawBinlogEvent>
            rawBinlogEventLinkedBlockingQueue2 = new  LinkedBlockingQueue<>();

    private static PipelinePosition pipelinePosition = new PipelinePosition(
            "localhost",
            12345,
            "binlog.000001",
            4L,
            "binlog.000001",
            4L
    );

    private static Configuration configuration;
    private static Applier applier = new DummyApplier();
    private static ReplicantPool replicantPool = new DummyReplicantPool();
    private BinlogEventProducer binlogEventProducer;

    private static void configure() throws IOException {
        String configName = "sampleConfiguration.yaml";
        args[0] = "-c" + configName;
        OptionSet optionSet = Cmd.parseArgs(args);
        StartupParameters startupParameters = new StartupParameters(optionSet);
        String  configPath = startupParameters.getConfigPath();

        ClassLoader classLoader = PipelineOrchestratorTest.class.getClassLoader();
        InputStream in = classLoader.getResourceAsStream(configPath);
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        configuration = mapper.readValue(in, com.booking.replication.Configuration.class);
        configuration.loadStartupParameters(startupParameters);

    }

    // TODO: add the same for binlog connector
    @Before
    public void setUp() throws Exception {
        if (configuration != null) return;
        configure();
        binlogEventProducer = new BinlogEventProducer(
                rawBinlogEventLinkedBlockingQueue2,
                pipelinePosition,
                configuration,
                replicantPool,
                BinlogEventParserProviderCode.OR
        );
    }

    @Test
    public void beginTransactionManual() throws Exception {

        PipelineOrchestrator pipelineOrchestrator = new PipelineOrchestrator(
                rawBinlogEventLinkedBlockingQueue,
                pipelinePosition,
                configuration,
                new DummyActiveSchemaVersion(),
                applier,
                replicantPool,
                binlogEventProducer,
                0L
        );

        assertFalse(pipelineOrchestrator.isInTransaction());

        pipelineOrchestrator.beginTransaction();
        assertTrue(pipelineOrchestrator.isInTransaction());
    }

    // TODO: the same for binlog connector
    @Test
    public void beginTransactionQueryEvent() throws Exception {

        RawBinlogEventQuery queryEvent = new RawBinlogEventQuery(
                new QueryEvent(new BinlogEventV4HeaderImpl())
        );

        queryEvent.setSql("BEGIN");

        PipelineOrchestrator pipelineOrchestrator = new PipelineOrchestrator(
                rawBinlogEventLinkedBlockingQueue,
                pipelinePosition,
                configuration,
                new DummyActiveSchemaVersion(),
                applier,
                replicantPool,
                binlogEventProducer,
                0L
        );

        assertFalse(pipelineOrchestrator.isInTransaction());

        pipelineOrchestrator.beginTransaction(queryEvent);

        assertTrue(pipelineOrchestrator.isInTransaction());
    }

    // TODO: the same for binlog connector
    @Test
    public void addEventIntoTransaction() throws Exception {

        RawBinlogEventRows queryEvent = new RawBinlogEventRows(
                new WriteRowsEventV2(new BinlogEventV4HeaderImpl())
        );

        PipelineOrchestrator pipelineOrchestrator = new PipelineOrchestrator(
                rawBinlogEventLinkedBlockingQueue,
                pipelinePosition,
                configuration,
                new DummyActiveSchemaVersion(),
                applier,
                replicantPool,
                binlogEventProducer,
                0L
        );

        pipelineOrchestrator.beginTransaction();

        assertFalse(pipelineOrchestrator.getCurrentTransaction().hasEvents());

        pipelineOrchestrator.addEventIntoTransaction(queryEvent);

        assertTrue(pipelineOrchestrator.getCurrentTransaction().hasEvents());
    }

    // TODO: the same for binlog connector
    @Test(expected = TransactionSizeLimitException.class)
    public void TransactionSizeLimitExceeded() throws Exception {

        PipelineOrchestrator pipelineOrchestrator = new PipelineOrchestrator(
                rawBinlogEventLinkedBlockingQueue,
                pipelinePosition,
                configuration,
                new DummyActiveSchemaVersion(),
                applier,
                replicantPool,
                binlogEventProducer,
                0L
        );

        Field orchestratorConfigurationField= pipelineOrchestrator.getClass().getDeclaredField("orchestratorConfiguration");
        orchestratorConfigurationField.setAccessible(true);
        Configuration.OrchestratorConfiguration orchestratorConfiguration = (Configuration.OrchestratorConfiguration) orchestratorConfigurationField.get(pipelineOrchestrator);

        pipelineOrchestrator.beginTransaction();

        assertFalse(pipelineOrchestrator.getCurrentTransaction().hasEvents());

        for (int i = 0; i < orchestratorConfiguration.getRewindingThreshold() + 2; i++) {
            RawBinlogEventRows queryEvent = new RawBinlogEventRows(
                    new WriteRowsEventV2(new BinlogEventV4HeaderImpl())
            );
            pipelineOrchestrator.addEventIntoTransaction(queryEvent);
        }
    }

    @Test
    public void isInTransaction() throws Exception {
        PipelineOrchestrator pipelineOrchestrator = new PipelineOrchestrator(
                rawBinlogEventLinkedBlockingQueue,
                pipelinePosition,
                configuration,
                new DummyActiveSchemaVersion(),
                applier,
                replicantPool,
                binlogEventProducer,
                0L
        );
        assertFalse(pipelineOrchestrator.isInTransaction());
        pipelineOrchestrator.beginTransaction();
        assertTrue(pipelineOrchestrator.isInTransaction());
    }

    @Test
    public void commitTransactionManual() throws Exception {
        PipelineOrchestrator pipelineOrchestrator = new PipelineOrchestrator(
                rawBinlogEventLinkedBlockingQueue,
                pipelinePosition,
                configuration,
                new DummyActiveSchemaVersion(),
                applier,
                replicantPool,
                binlogEventProducer,
                0L
        );
        assertFalse(pipelineOrchestrator.isInTransaction());
        pipelineOrchestrator.beginTransaction();
        assertTrue(pipelineOrchestrator.isInTransaction());
        pipelineOrchestrator.commitTransaction(0L, 0L);
        assertFalse(pipelineOrchestrator.isInTransaction());
    }

    // TODO: the same for binlog connector
    @Test
    public void commitTransactionQueryEvent() throws Exception {

        BinlogEventV4HeaderImpl commitEventHeader = new BinlogEventV4HeaderImpl();
        commitEventHeader.setEventType(MySQLConstants.QUERY_EVENT);

        RawBinlogEventQuery queryEvent = new RawBinlogEventQuery(
                new QueryEvent(commitEventHeader)
        );

        queryEvent.setSql("COMMIT");

        PipelineOrchestrator pipelineOrchestrator = new PipelineOrchestrator(
                rawBinlogEventLinkedBlockingQueue,
                pipelinePosition,
                configuration,
                new DummyActiveSchemaVersion(),
                applier,
                replicantPool,
                binlogEventProducer,
                0L
        );

        assertFalse(pipelineOrchestrator.isInTransaction());

        pipelineOrchestrator.beginTransaction();
        assertTrue(pipelineOrchestrator.isInTransaction());

        pipelineOrchestrator.commitTransaction(queryEvent);
        assertFalse(pipelineOrchestrator.isInTransaction());
    }

    // TODO: the same for binlog connector
    @Test
    public void commitTransactionXidEvent() throws Exception {

        BinlogEventV4HeaderImpl xidEventHeader = new BinlogEventV4HeaderImpl();
        xidEventHeader.setEventType(MySQLConstants.XID_EVENT);
        RawBinlogEventXid xidEvent = new RawBinlogEventXid(
                new XidEvent(xidEventHeader)
        );

        PipelineOrchestrator pipelineOrchestrator = new PipelineOrchestrator(
                rawBinlogEventLinkedBlockingQueue,
                pipelinePosition,
                configuration,
                new DummyActiveSchemaVersion(),
                applier,
                replicantPool,
                binlogEventProducer,
                0L
        );

        assertFalse(pipelineOrchestrator.isInTransaction());

        pipelineOrchestrator.beginTransaction();
        assertTrue(pipelineOrchestrator.isInTransaction());

        pipelineOrchestrator.commitTransaction(xidEvent);
        assertFalse(pipelineOrchestrator.isInTransaction());
    }

    // TODO: the same for binlog connector
    @Test(timeout=1000)
    public void waitForEvent() throws Exception {

        PipelineOrchestrator pipelineOrchestrator = new PipelineOrchestrator(
                rawBinlogEventLinkedBlockingQueue,
                pipelinePosition,
                configuration,
                new DummyActiveSchemaVersion(),
                applier,
                replicantPool,
                binlogEventProducer,
                0L
        );

        pipelineOrchestrator.setRunning(true);

        Method method = pipelineOrchestrator.getClass().getDeclaredMethod("waitForEvent", long.class, long.class);
        method.setAccessible(true);
        final RawBinlogEvent[] events = new RawBinlogEvent[1];
        RawBinlogEvent queryEvent = new RawBinlogEvent(
                new QueryEvent(new BinlogEventV4HeaderImpl())
        );

        Thread pipelineThread = new Thread(() -> {
            try {
                events[0] = (RawBinlogEvent) method.invoke(pipelineOrchestrator, 0,0);
            } catch (IllegalAccessException | InvocationTargetException e) {
                e.printStackTrace();
            }
        });

        pipelineThread.start();
        assertNull(events[0]);
        rawBinlogEventLinkedBlockingQueue.add(queryEvent);
        while (events[0] == null) {
            Thread.sleep(20);
        }

        assertEquals(queryEvent, events[0]);
    }

    // TODO: do the same for the binlog connector
    @Test
    public void rewindToCommitEventQueryEvent() throws Exception {

        PipelineOrchestrator pipelineOrchestrator = new PipelineOrchestrator(
                rawBinlogEventLinkedBlockingQueue,
                pipelinePosition,
                configuration,
                new DummyActiveSchemaVersion(),
                applier,
                replicantPool,
                binlogEventProducer,
                0L
        );

        pipelineOrchestrator.setRunning(true);
        Method method = pipelineOrchestrator.getClass().getDeclaredMethod("rewindToCommitEvent", long.class, long.class);
        method.setAccessible(true);
        final RawBinlogEvent[] events = new RawBinlogEvent[1];

        BinlogEventV4HeaderImpl commitEventHeader = new BinlogEventV4HeaderImpl();
        commitEventHeader.setEventType(MySQLConstants.QUERY_EVENT);
        RawBinlogEventQuery commitEvent = new RawBinlogEventQuery(
                new QueryEvent(commitEventHeader)
        );

        commitEvent.setSql("COMMIT");

        Thread pipelineThread = new Thread(() -> {
            try {
                events[0] = (RawBinlogEvent) method.invoke(pipelineOrchestrator, 0, 0);
            } catch (IllegalAccessException | InvocationTargetException e) {
                e.printStackTrace();
            }
        });


        pipelineThread.start();
        assertNull(events[0]);
        for (int i = 0; i < 20; i++) {
            rawBinlogEventLinkedBlockingQueue.add(new RawBinlogEventRows(
                    new WriteRowsEventV2(new BinlogEventV4HeaderImpl()))
            );
        }
        rawBinlogEventLinkedBlockingQueue.add(commitEvent);
        while (events[0] == null) {
            Thread.sleep(5);
        }

        assertEquals(commitEvent, events[0]);
    }

    // TODO: add the same for binlog connector
    @Test
    public void rewindToCommitEventXidEvent() throws Exception {
        PipelineOrchestrator pipelineOrchestrator = new PipelineOrchestrator(
                rawBinlogEventLinkedBlockingQueue,
                pipelinePosition,
                configuration,
                new DummyActiveSchemaVersion(),
                applier,
                replicantPool,
                binlogEventProducer,
                0L
        );
        pipelineOrchestrator.setRunning(true);
        Method method = pipelineOrchestrator.getClass().getDeclaredMethod("rewindToCommitEvent", long.class, long.class);
        method.setAccessible(true);
        final RawBinlogEvent[] events = new RawBinlogEvent[1];

        BinlogEventV4HeaderImpl xidEventHeader = new BinlogEventV4HeaderImpl();
        xidEventHeader.setEventType(MySQLConstants.XID_EVENT);
        RawBinlogEventXid commitEvent = new RawBinlogEventXid(
                new XidEvent(xidEventHeader)
        );

        Thread pipelineThread = new Thread(() -> {
            try {
                events[0] = (RawBinlogEvent) method.invoke(pipelineOrchestrator, 0, 0);
            } catch (IllegalAccessException | InvocationTargetException e) {
                e.printStackTrace();
            }
        });

        pipelineThread.start();
        assertNull(events[0]);

        for (int i = 0; i < 20; i++) {
            rawBinlogEventLinkedBlockingQueue.add(new RawBinlogEventRows(
                    new WriteRowsEventV2(new BinlogEventV4HeaderImpl()))
            );
        }

        rawBinlogEventLinkedBlockingQueue.add(commitEvent);
        while (events[0] == null) {
            Thread.sleep(5);
        }

        assertEquals(commitEvent, events[0]);
    }

}