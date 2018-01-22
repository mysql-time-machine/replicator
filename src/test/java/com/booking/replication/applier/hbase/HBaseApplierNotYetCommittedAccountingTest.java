package com.booking.replication.applier.hbase;

import com.booking.replication.applier.TaskStatus;
import com.booking.replication.checkpoints.LastCommittedPositionCheckpoint;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.*;
import java.util.concurrent.*;

import static org.junit.Assert.*;

/**
 * Created by smalviya on 1/15/18.
 */
public class HBaseApplierNotYetCommittedAccountingTest {

    private HBaseApplierNotYetCommittedAccounting hbAccounting;


    @Before
    public void initialize() throws Exception {
        hbAccounting = new HBaseApplierNotYetCommittedAccounting();
    }

    @Test
    public void addTaskUUID() throws Exception {
        int threadCount = 10;

        class Task implements Runnable {

            private HBaseApplierNotYetCommittedAccounting hbAccounting;
            private List<String> expected;

            public Task(HBaseApplierNotYetCommittedAccounting hbAccounting, List<String> expected) {
                this.hbAccounting = hbAccounting;
                this.expected = expected;
            }

            @Override
            public void run() {
                try {
                    Thread.sleep((long)(Math.random() * 1000));
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                hbAccounting.addTaskUUID(Long.toString(Thread.currentThread().getId()));
                expected.add(Long.toString(Thread.currentThread().getId()));
            }
        }

        ExecutorService executorService = Executors.newFixedThreadPool(threadCount);

        // List to generate expected values
        List expected = Collections.synchronizedList(new ArrayList());

        for (Integer i=0; i<threadCount; i++) {
            Runnable task = new Task(hbAccounting, expected);
            executorService.execute(task);
        }

        executorService.shutdown();
        // Wait until all threads are finish
        while (!executorService.isTerminated()) {

        }

        // Using reflection to check the against the actual field value
        Field notYetCommittedTaskUUIDs = HBaseApplierNotYetCommittedAccounting.class.getDeclaredField("notYetCommittedTaskUUIDs");
        notYetCommittedTaskUUIDs.setAccessible(true);
        List actual = (List) notYetCommittedTaskUUIDs.get(hbAccounting);

        assertEquals(expected, actual);
    }

    @Test
    public void containsTaskUUID() throws Exception {
        List expected = new ArrayList();
        Set expectedSet = new HashSet();
        int tests = 100;
        for (int i=0; i<tests; i++) {
            String rand = Double.toString(Math.random());
            hbAccounting.addTaskUUID(rand);
            expected.add(rand);
            expectedSet.add(rand);
        }

        for (int i=0; i<tests; i++) {
            Random random = new Random();
            boolean checkExpected = random.nextBoolean();
            if (checkExpected) {
                String testVal = (String) expected.get(random.nextInt(expected.size()));
                assertTrue(hbAccounting.containsTaskUUID(testVal));
            }
            else {
                String testVal = Double.toString(random.nextDouble());
                if (!expectedSet.contains(testVal)) {
                    assertFalse(hbAccounting.containsTaskUUID(testVal));
                }
            }
        }

    }

    @Test
    public void doAccountingOnTaskSuccess_simple1() throws Exception {
        ConcurrentHashMap<String, ApplierTask> taskTransactionBuffer = new ConcurrentHashMap();

        // Initialize task buffer
        taskTransactionBuffer.putIfAbsent("1", new ApplierTask(TaskStatus.TASK_SUBMITTED));
        taskTransactionBuffer.putIfAbsent("2", new ApplierTask(TaskStatus.TASK_SUBMITTED));
        taskTransactionBuffer.putIfAbsent("3", new ApplierTask(TaskStatus.TASK_SUBMITTED));
        taskTransactionBuffer.putIfAbsent("4", new ApplierTask(TaskStatus.TASK_SUBMITTED));
        hbAccounting.addTaskUUID("1");
        hbAccounting.addTaskUUID("2");
        hbAccounting.addTaskUUID("3");
        hbAccounting.addTaskUUID("4");

        assertEquals(null, hbAccounting.doAccountingOnTaskSuccess(taskTransactionBuffer, "4"));

    }

    @Test
    public void doAccountingOnTaskSuccess_simple2() throws Exception {
        ConcurrentHashMap<String, ApplierTask> taskTransactionBuffer = new ConcurrentHashMap();

        // Create a dummy checkpoint
        ApplierTask checkpointTask = new ApplierTask(TaskStatus.WRITE_SUCCEEDED);
        checkpointTask.setPseudoGTIDCheckPoint(new LastCommittedPositionCheckpoint());


        // Initialize task buffer
        taskTransactionBuffer.putIfAbsent("1", checkpointTask);
        taskTransactionBuffer.putIfAbsent("2", new ApplierTask(TaskStatus.WRITE_SUCCEEDED));
        taskTransactionBuffer.putIfAbsent("3", new ApplierTask(TaskStatus.WRITE_SUCCEEDED));
        taskTransactionBuffer.putIfAbsent("4", new ApplierTask(TaskStatus.WRITE_SUCCEEDED));
        hbAccounting.addTaskUUID("1");
        hbAccounting.addTaskUUID("2");
        hbAccounting.addTaskUUID("3");
        hbAccounting.addTaskUUID("4");

        assertNotNull(hbAccounting.doAccountingOnTaskSuccess(taskTransactionBuffer, "4"));

    }

    @Test
    public void doAccountingOnTaskSuccess_simple3() throws Exception {
        ConcurrentHashMap<String, ApplierTask> taskTransactionBuffer = new ConcurrentHashMap();

        // Create a dummy checkpoint
        ApplierTask dummyCheckpointTask = new ApplierTask(TaskStatus.WRITE_SUCCEEDED);
        dummyCheckpointTask.setPseudoGTIDCheckPoint(new LastCommittedPositionCheckpoint());

        ApplierTask expectedCheckpointTask = new ApplierTask(TaskStatus.WRITE_SUCCEEDED);
        expectedCheckpointTask.setPseudoGTIDCheckPoint(new LastCommittedPositionCheckpoint());

        // Initialize task buffer
        taskTransactionBuffer.putIfAbsent("1", dummyCheckpointTask);
        taskTransactionBuffer.putIfAbsent("2", new ApplierTask(TaskStatus.WRITE_SUCCEEDED));
        taskTransactionBuffer.putIfAbsent("3", new ApplierTask(TaskStatus.WRITE_SUCCEEDED));
        taskTransactionBuffer.putIfAbsent("4", expectedCheckpointTask);
        hbAccounting.addTaskUUID("1");
        hbAccounting.addTaskUUID("2");
        hbAccounting.addTaskUUID("3");
        hbAccounting.addTaskUUID("4");

        assertEquals(expectedCheckpointTask.getPseudoGTIDCheckPoint(), hbAccounting.doAccountingOnTaskSuccess(taskTransactionBuffer, "4"));

    }

/* TODO Test to mimick the behaviour of multiple threads picking up submitted tasks. The problem is in some cases two threads can pick up the same task from the taskBuffer. Have to think about a better way to do this.
     */
//    @Test
//    public void doAccountingOnTaskSuccess_threaded() throws Exception {
//        int threadCount = 3;
//        int numTasks = 10;
//
//        ConcurrentHashMap<String, ApplierTask> taskTransactionBuffer = new ConcurrentHashMap();
//        // Create a dummy checkpoint
//        ApplierTask dummyCheckpointTask = new ApplierTask(TaskStatus.TASK_SUBMITTED);
//        dummyCheckpointTask.setPseudoGTIDCheckPoint(new LastCommittedPositionCheckpoint());
//        ApplierTask expectedCheckpointTask = dummyCheckpointTask;
//
//        // Initialize task buffer
//        for(int i=0; i<numTasks; i++) {
//            hbAccounting.addTaskUUID(Integer.toString(i));
//            if (i == 0) {
//                taskTransactionBuffer.putIfAbsent(Integer.toString(i), dummyCheckpointTask);
//            }
//            else {
//                taskTransactionBuffer.putIfAbsent(Integer.toString(i), new ApplierTask(TaskStatus.TASK_SUBMITTED));
//            }
//        }
//
//        // Accounting task for a thread
//        class Task implements Runnable {
//
//            private HBaseApplierNotYetCommittedAccounting hbAccounting;
//            private ConcurrentHashMap<String, ApplierTask> taskTransactionBuffer;
//            private ApplierTask expectedCheckpointTask;
//
//            public Task(HBaseApplierNotYetCommittedAccounting hbAccounting, ConcurrentHashMap<String, ApplierTask> taskTransactionBuffer, ApplierTask expectedCheckpointTask) {
//                this.hbAccounting = hbAccounting;
//                this.taskTransactionBuffer = taskTransactionBuffer;
//                this.expectedCheckpointTask = expectedCheckpointTask;
//            }
//
//            @Override
//            public void run() {
//                try {
//                    Thread.sleep((long)(Math.random() * 1000));
//                } catch (InterruptedException e) {
//                    e.printStackTrace();
//                }
//                try {
////                    while( getSubmittedTask(this.taskTransactionBuffer)!= null) {
//                        System.out.println("Thread id:" + Thread.currentThread().getId() + " Task id: " + getSubmittedTask(this.taskTransactionBuffer));
//                        expectedCheckpointTask.setPseudoGTIDCheckPoint(hbAccounting.doAccountingOnTaskSuccess(this.taskTransactionBuffer, getSubmittedTask(this.taskTransactionBuffer)));
////                    }
//                } catch (Exception e) {
//                    e.printStackTrace();
//                }
//            }
//        }
//
//        ExecutorService executorService = Executors.newFixedThreadPool(threadCount);
//        for (Integer i=0; i<threadCount; i++) {
//            Runnable task = new Task(hbAccounting, taskTransactionBuffer, expectedCheckpointTask);
//            executorService.execute(task);
//        }
//
//        executorService.shutdown();
//        // Wait until all threads are finish
//        while (!executorService.isTerminated()) {
//
//        }
//
//        assertNotSame(expectedCheckpointTask.getPseudoGTIDCheckPoint(), dummyCheckpointTask.getPseudoGTIDCheckPoint());
//
//    }
//    private synchronized String getSubmittedTask(ConcurrentHashMap<String, ApplierTask> taskTransactionBuffer) {
//        String committedTaskId = taskTransactionBuffer.search(1, (k, v) -> {
//            if (v.getTaskStatus() == TaskStatus.TASK_SUBMITTED) {
//                return k;
//            }
//            return null;
//        });
//        return committedTaskId;
//    }
}