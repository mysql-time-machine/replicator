package com.booking.replication.applier.hbase.mutation;

import com.booking.replication.applier.hbase.HBaseApplier;
import com.booking.replication.augmenter.model.event.AugmentedEventType;
import com.booking.replication.augmenter.model.row.AugmentedRow;
import com.booking.replication.commons.metrics.Metrics;
import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.util.Bytes;
import org.eclipse.jetty.server.Server;
import org.junit.Assert;
import org.junit.Test;

import java.util.*;

public class MutationGeneratorTest {

    // TODO: timestamp organizer test
    // TODO: delete event test

    @Test
    public void mutationGeneratorInsertTest() {

        Map<String, Object> configuration = getConfiguration();

        Metrics<?> metrics = Metrics.build(configuration, new Server());

        AugmentedRow testInsertRow = getTestInsertAugmentedRow();

        HBaseApplierMutationGenerator mutationGenerator = new HBaseApplierMutationGenerator(configuration, metrics);

        HBaseApplierMutationGenerator.PutMutation insertMutation =
                mutationGenerator.getPutForMirroredTable(testInsertRow);

        String expectedRowKey = "202cb962;123;456";
        Long expectedTimestamp = testInsertRow.getRowMicrosecondTimestamp();
        String expectedRowStatus = "I";

        Map<String, String> expectedValues = ImmutableMap.of(
                "id", "123",
                "ca", "456",
                "cb", "789",
                "cc", "987",
                "_transaction_uuid", "85503b0c-ae28-473a-ae70-86af3e61458b:1234567"

        );

        String resultRowKey = Bytes.toString(insertMutation.getPut().getRow());
    
        Map<String, String> resultValues = new HashMap<>();
        List<Long> cellMutationTimestamps = new ArrayList<>();
        List<Cell> cells = insertMutation.getPut().getFamilyCellMap().get(Bytes.toBytes("d"));
        ListIterator<Cell> cellsIterator = cells.listIterator();
        while (cellsIterator.hasNext()) {
            Cell cell = cellsIterator.next();
            String cellQualifier =
                    Bytes.toString(
                            Arrays.copyOfRange(
                                    cell.getQualifierArray(),
                                    cell.getQualifierOffset(),
                                    cell.getQualifierOffset() + cell.getQualifierLength()
                            )
                    );
            String cellValue =
                    Bytes.toString(
                        Arrays.copyOfRange(
                            cell.getValueArray(),
                            cell.getValueOffset(),
                            cell.getValueOffset() + cell.getValueLength()
                        )
                );

            Long cellMutationTimestamp = cell.getTimestamp();
            cellMutationTimestamps.add(cellMutationTimestamp);

            resultValues.put(cellQualifier,cellValue);
        }

        Assert.assertEquals(expectedRowKey, resultRowKey);
        for (Long cellMutationTimestamp: cellMutationTimestamps) {
            Assert.assertEquals(expectedTimestamp, cellMutationTimestamp);
        }
        for (String cellQualifier: expectedValues.keySet()) {
            Assert.assertTrue(resultValues.containsKey(cellQualifier));
            Assert.assertEquals(expectedValues.get(cellQualifier),resultValues.get(cellQualifier));
        }
        Assert.assertTrue(resultValues.containsKey("row_status"));
        Assert.assertEquals(expectedRowStatus, resultValues.get("row_status"));

    }

    @Test
    public void mutationGeneratorUpdateTest() {

        Map<String, Object> configuration = getConfiguration();

        Metrics<?> metrics = Metrics.build(configuration, new Server());

        AugmentedRow testUpdateRow = getTestUpdateAugmentedRow();

        HBaseApplierMutationGenerator mutationGenerator = new HBaseApplierMutationGenerator(configuration, metrics);

        HBaseApplierMutationGenerator.PutMutation updateMutation =
                mutationGenerator.getPutForMirroredTable(testUpdateRow);

        String expectedRowKey = "202cb962;123;456";
        Long expectedTimestamp = testUpdateRow.getRowMicrosecondTimestamp();
        String expectedRowStatus = "U";

        Map<String, String> expectedValues = ImmutableMap.of(
                "cb", "800",
                "_transaction_uuid", "85503b0c-ae28-473a-ae70-86af3e61458b:1234568",
                "_transaction_xid", "0",
                "row_status", "U"

        );

        String resultRowKey = Bytes.toString(updateMutation.getPut().getRow());

        Map<String, String> resultValues = new HashMap<>();
        List<Long> cellMutationTimestamps = new ArrayList<>();
        List<Cell> cells = updateMutation.getPut().getFamilyCellMap().get(Bytes.toBytes("d"));
        ListIterator<Cell> cellsIterator = cells.listIterator();
        while (cellsIterator.hasNext()) {
            Cell cell = cellsIterator.next();
            String cellQualifier =
                    Bytes.toString(
                            Arrays.copyOfRange(
                                    cell.getQualifierArray(),
                                    cell.getQualifierOffset(),
                                    cell.getQualifierOffset() + cell.getQualifierLength()
                            )
                    );
            String cellValue =
                    Bytes.toString(
                            Arrays.copyOfRange(
                                    cell.getValueArray(),
                                    cell.getValueOffset(),
                                    cell.getValueOffset() + cell.getValueLength()
                            )
                    );

            resultValues.put(cellQualifier,cellValue);

            Long cellMutationTimestamp = cell.getTimestamp();
            cellMutationTimestamps.add(cellMutationTimestamp);

        }

        Assert.assertEquals(expectedRowKey, resultRowKey);
        for (Long cellMutationTimestamp: cellMutationTimestamps) {
            Assert.assertEquals(expectedTimestamp, cellMutationTimestamp);
        }
        for (String cellQualifier: expectedValues.keySet()) {
            Assert.assertTrue(resultValues.containsKey(cellQualifier));
            Assert.assertEquals(expectedValues.get(cellQualifier),resultValues.get(cellQualifier));
        }
        Assert.assertTrue(!resultValues.containsKey("cc"));

    }


    private AugmentedRow getTestInsertAugmentedRow() {

        Long commitTimestamp = 1575566080000L;
        Long transactionSequenceNumber = 100L;

        AugmentedRow ar = new AugmentedRow(
                AugmentedEventType.INSERT,
                "test",
                "SomeTable",
                commitTimestamp,
                "85503b0c-ae28-473a-ae70-86af3e61458b:1234567",
                0L,
                Arrays.asList("id", "ca"),
                ImmutableMap.of(
                        "id", 123,
                        "ca", 456,
                        "cb", 789,
                        "cc", 987
                )
        );

        ar.setTransactionSequenceNumber(transactionSequenceNumber);

        Long microsOverride = commitTimestamp * 1000 + ar.getTransactionSequenceNumber();

        ar.setRowMicrosecondTimestamp(microsOverride);

        return  ar;
    }

    private AugmentedRow getTestUpdateAugmentedRow() {

        Long commitTimestamp = 1575566080000L;
        Long transactionSequenceNumber = 101L;

        Map<String, Object> values = ImmutableMap.of(
                "id", ImmutableMap.of("a", 123, "b", 123),
                "ca", ImmutableMap.of("a", 456, "b", 456),
                "cb", ImmutableMap.of("a", 800, "b", 789), // <- change
                "cc",  ImmutableMap.of("a", 987, "b", 987)
        );

        AugmentedRow ar = new AugmentedRow(
                AugmentedEventType.UPDATE,
                "test",
                "SomeTable",
                commitTimestamp,
                "85503b0c-ae28-473a-ae70-86af3e61458b:1234568",
                0L,
                Arrays.asList("id", "ca"),
                values
        );

        ar.setTransactionSequenceNumber(transactionSequenceNumber);

        Long microsOverride = commitTimestamp * 1000 + ar.getTransactionSequenceNumber();

        System.out.println(microsOverride);
        ar.setRowMicrosecondTimestamp(microsOverride);

        return  ar;
    }

    private Map<String, Object> getConfiguration() {

        Map<String, Object> configuration = new HashMap<>();

        configuration.put(HBaseApplier.Configuration.TARGET_NAMESPACE,  "namespaceTest");
        configuration.put(HBaseApplier.Configuration.PAYLOAD_TABLE_NAME, "payloadTableTest");

        configuration.put(Metrics.Configuration.TYPE, Metrics.Type.CONSOLE.name());

        return configuration;
    }
}
