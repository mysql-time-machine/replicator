package com.booking.replication.pipeline;

import com.booking.replication.Configuration;
import com.booking.replication.applier.Applier;
import com.booking.replication.applier.DummyApplier;
import com.booking.replication.pipeline.event.handler.TransactionSizeLimitException;
import com.booking.replication.queues.ReplicatorQueues;
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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static org.junit.Assert.*;


/**
 * Created by edmitriev on 8/2/17.
 */
public class PipelineOrchestratorTest {
    private static String[] args = new String[1];

    private static ReplicatorQueues replicatorQueues = new ReplicatorQueues();
    private static BlockingQueue<BinlogEventV4> queue = new LinkedBlockingQueue<>();
    private static PipelinePosition pipelinePosition = new PipelinePosition("localhost", 12345, "binlog.000001", 4L, "binlog.000001", 4L);
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

        PipelineOrchestrator.setActiveSchemaVersion(new DummyActiveSchemaVersion());
    }

    @Before
    public void setUp() throws Exception {
        if (configuration != null) return;
        configure();
        binlogEventProducer = new BinlogEventProducer(queue, pipelinePosition, configuration, replicantPool);
    }

    @Test
    public void beginTransactionManual() throws Exception {
        PipelineOrchestrator pipelineOrchestrator = new PipelineOrchestrator(replicatorQueues, pipelinePosition, configuration, applier, replicantPool, binlogEventProducer, 0L, false);
        assertFalse(pipelineOrchestrator.isInTransaction());
        pipelineOrchestrator.beginTransaction();
        assertTrue(pipelineOrchestrator.isInTransaction());
    }

    @Test
    public void beginTransactionQueryEvent() throws Exception {
        QueryEvent queryEvent = new QueryEvent(new BinlogEventV4HeaderImpl());
        Constructor<StringColumn> stringColumnReflection = StringColumn.class.getDeclaredConstructor(byte[].class);
        stringColumnReflection.setAccessible(true);
        StringColumn stringColumn = stringColumnReflection.newInstance((Object) "BEGIN".getBytes());
        queryEvent.setSql(stringColumn);

        PipelineOrchestrator pipelineOrchestrator = new PipelineOrchestrator(replicatorQueues, pipelinePosition, configuration, applier, replicantPool, binlogEventProducer, 0L, false);
        assertFalse(pipelineOrchestrator.isInTransaction());
        pipelineOrchestrator.beginTransaction(queryEvent);
        assertTrue(pipelineOrchestrator.isInTransaction());
    }

    @Test
    public void addEventIntoTransaction() throws Exception {
        WriteRowsEventV2 queryEvent = new WriteRowsEventV2(new BinlogEventV4HeaderImpl());

        PipelineOrchestrator pipelineOrchestrator = new PipelineOrchestrator(replicatorQueues, pipelinePosition, configuration, applier, replicantPool, binlogEventProducer, 0L, false);
        pipelineOrchestrator.beginTransaction();

        assertFalse(pipelineOrchestrator.getCurrentTransaction().hasEvents());
        pipelineOrchestrator.addEventIntoTransaction(queryEvent);
        assertTrue(pipelineOrchestrator.getCurrentTransaction().hasEvents());
    }

    @Test(expected = TransactionSizeLimitException.class)
    public void TransactionSizeLimitExceeded() throws Exception {
        PipelineOrchestrator pipelineOrchestrator = new PipelineOrchestrator(replicatorQueues, pipelinePosition, configuration, applier, replicantPool, binlogEventProducer, 0L, false);
        Field orchestratorConfigurationField= pipelineOrchestrator.getClass().getDeclaredField("orchestratorConfiguration");
        orchestratorConfigurationField.setAccessible(true);
        Configuration.OrchestratorConfiguration orchestratorConfiguration = (Configuration.OrchestratorConfiguration) orchestratorConfigurationField.get(pipelineOrchestrator);

        pipelineOrchestrator.beginTransaction();

        assertFalse(pipelineOrchestrator.getCurrentTransaction().hasEvents());
        for (int i = 0; i < orchestratorConfiguration.getRewindingThreshold() + 2; i++) {
            WriteRowsEventV2 queryEvent = new WriteRowsEventV2(new BinlogEventV4HeaderImpl());
            pipelineOrchestrator.addEventIntoTransaction(queryEvent);
        }
    }

    @Test
    public void isInTransaction() throws Exception {
        PipelineOrchestrator pipelineOrchestrator = new PipelineOrchestrator(replicatorQueues, pipelinePosition, configuration, applier, replicantPool, binlogEventProducer, 0L, false);
        assertFalse(pipelineOrchestrator.isInTransaction());
        pipelineOrchestrator.beginTransaction();
        assertTrue(pipelineOrchestrator.isInTransaction());
    }

    @Test
    public void commitTransactionManual() throws Exception {
        PipelineOrchestrator pipelineOrchestrator = new PipelineOrchestrator(replicatorQueues, pipelinePosition, configuration, applier, replicantPool, binlogEventProducer, 0L, false);
        assertFalse(pipelineOrchestrator.isInTransaction());
        pipelineOrchestrator.beginTransaction();
        assertTrue(pipelineOrchestrator.isInTransaction());
        pipelineOrchestrator.commitTransaction(0L, 0L);
        assertFalse(pipelineOrchestrator.isInTransaction());
    }

    @Test
    public void commitTransactionQueryEvent() throws Exception {
        QueryEvent queryEvent = new QueryEvent(new BinlogEventV4HeaderImpl());
        Constructor<StringColumn> stringColumnReflection = StringColumn.class.getDeclaredConstructor(byte[].class);
        stringColumnReflection.setAccessible(true);
        StringColumn stringColumn = stringColumnReflection.newInstance((Object) "COMMIT".getBytes());
        queryEvent.setSql(stringColumn);
        ((BinlogEventV4HeaderImpl) queryEvent.getHeader()).setEventType(MySQLConstants.QUERY_EVENT);

        PipelineOrchestrator pipelineOrchestrator = new PipelineOrchestrator(replicatorQueues, pipelinePosition, configuration, applier, replicantPool, binlogEventProducer, 0L, false);
        assertFalse(pipelineOrchestrator.isInTransaction());
        pipelineOrchestrator.beginTransaction();
        assertTrue(pipelineOrchestrator.isInTransaction());
        pipelineOrchestrator.commitTransaction(queryEvent);
        assertFalse(pipelineOrchestrator.isInTransaction());
    }

    @Test
    public void commitTransactionXidEvent() throws Exception {
        XidEvent xidEvent = new XidEvent(new BinlogEventV4HeaderImpl());
        ((BinlogEventV4HeaderImpl) xidEvent.getHeader()).setEventType(MySQLConstants.XID_EVENT);

        PipelineOrchestrator pipelineOrchestrator = new PipelineOrchestrator(replicatorQueues, pipelinePosition, configuration, applier, replicantPool, binlogEventProducer, 0L, false);
        assertFalse(pipelineOrchestrator.isInTransaction());
        pipelineOrchestrator.beginTransaction();
        assertTrue(pipelineOrchestrator.isInTransaction());
        pipelineOrchestrator.commitTransaction(xidEvent);
        assertFalse(pipelineOrchestrator.isInTransaction());
    }

    @Test(timeout=1000)
    public void waitForEvent() throws Exception {
        PipelineOrchestrator pipelineOrchestrator = new PipelineOrchestrator(replicatorQueues, pipelinePosition, configuration, applier, replicantPool, binlogEventProducer, 0L, false);
        pipelineOrchestrator.setRunning(true);
        Method method = pipelineOrchestrator.getClass().getDeclaredMethod("waitForEvent", long.class, long.class);
        method.setAccessible(true);
        final BinlogEventV4[] events = new BinlogEventV4[1];
        WriteRowsEventV2 queryEvent = new WriteRowsEventV2(new BinlogEventV4HeaderImpl());

        Thread pipelineThread = new Thread(() -> {
            try {
                events[0] = (BinlogEventV4) method.invoke(pipelineOrchestrator, 0,0);
            } catch (IllegalAccessException | InvocationTargetException e) {
                e.printStackTrace();
            }
        });



        pipelineThread.start();
        assertNull(events[0]);
        replicatorQueues.rawQueue.add(queryEvent);
        while (events[0] == null) {
            Thread.sleep(20);
        }

        assertEquals(queryEvent, events[0]);
    }

    @Test
    public void rewindToCommitEventQueryEvent() throws Exception {
        PipelineOrchestrator pipelineOrchestrator = new PipelineOrchestrator(replicatorQueues, pipelinePosition, configuration, applier, replicantPool, binlogEventProducer, 0L, false);
        pipelineOrchestrator.setRunning(true);
        Method method = pipelineOrchestrator.getClass().getDeclaredMethod("rewindToCommitEvent", long.class, long.class);
        method.setAccessible(true);
        final BinlogEventV4[] events = new BinlogEventV4[1];

        QueryEvent commitEvent = new QueryEvent(new BinlogEventV4HeaderImpl());
        Constructor<StringColumn> stringColumnReflection = StringColumn.class.getDeclaredConstructor(byte[].class);
        stringColumnReflection.setAccessible(true);
        StringColumn stringColumn = stringColumnReflection.newInstance((Object) "COMMIT".getBytes());
        commitEvent.setSql(stringColumn);
        ((BinlogEventV4HeaderImpl) commitEvent.getHeader()).setEventType(MySQLConstants.QUERY_EVENT);

        Thread pipelineThread = new Thread(() -> {
            try {
                events[0] = (BinlogEventV4) method.invoke(pipelineOrchestrator, 0, 0);
            } catch (IllegalAccessException | InvocationTargetException e) {
                e.printStackTrace();
            }
        });


        pipelineThread.start();
        assertNull(events[0]);
        for (int i = 0; i < 20; i++) {
            replicatorQueues.rawQueue.add(new WriteRowsEventV2(new BinlogEventV4HeaderImpl()));
        }
        replicatorQueues.rawQueue.add(commitEvent);
        while (events[0] == null) {
            Thread.sleep(5);
        }

        assertEquals(commitEvent, events[0]);
    }

    @Test
    public void rewindToCommitEventXidEvent() throws Exception {
        PipelineOrchestrator pipelineOrchestrator = new PipelineOrchestrator(replicatorQueues, pipelinePosition, configuration, applier, replicantPool, binlogEventProducer, 0L, false);
        pipelineOrchestrator.setRunning(true);
        Method method = pipelineOrchestrator.getClass().getDeclaredMethod("rewindToCommitEvent", long.class, long.class);
        method.setAccessible(true);
        final BinlogEventV4[] events = new BinlogEventV4[1];

        XidEvent commitEvent = new XidEvent(new BinlogEventV4HeaderImpl());
        ((BinlogEventV4HeaderImpl) commitEvent.getHeader()).setEventType(MySQLConstants.XID_EVENT);

        Thread pipelineThread = new Thread(() -> {
            try {
                events[0] = (BinlogEventV4) method.invoke(pipelineOrchestrator, 0, 0);
            } catch (IllegalAccessException | InvocationTargetException e) {
                e.printStackTrace();
            }
        });


        pipelineThread.start();
        assertNull(events[0]);
        for (int i = 0; i < 20; i++) {
            replicatorQueues.rawQueue.add(new WriteRowsEventV2(new BinlogEventV4HeaderImpl()));
        }
        replicatorQueues.rawQueue.add(commitEvent);
        while (events[0] == null) {
            Thread.sleep(5);
        }

        assertEquals(commitEvent, events[0]);
    }

}