package com.booking.replication.applier.hbase.keygen;

import com.booking.replication.applier.hbase.schema.HBaseRowKeyMapper;
import com.booking.replication.augmenter.model.event.AugmentedEventType;
import com.booking.replication.augmenter.model.row.AugmentedRow;
import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class RowKeyMapperTest {

    @Test
    public void testRowKeySalt() {

        AugmentedRow augmentedRow = new AugmentedRow(
                AugmentedEventType.INSERT,
                "test",
                "SomeTable",
                1575566080000L,
                "85503b0c-ae28-473a-ae70-86af3e61458b:1234567",
                0L,
                Arrays.asList("id", "ca"),
                ImmutableMap.of(
                        "id", 123,
                        "ca", 456,
                        "cb", 789
                )
        );
        String expectedHBaseRowKey = "202cb962;123;456";

        String result = HBaseRowKeyMapper.getSaltedHBaseRowKey(augmentedRow);

        Assert.assertEquals("Salted key test", expectedHBaseRowKey, result);

    }

    @Test
    public void payloadTableRowKey() {
        AugmentedRow augmentedRow = new AugmentedRow(
                AugmentedEventType.INSERT,
                "test",
                "SomeTable",
                1575566080000L,
                "85503b0c-ae28-473a-ae70-86af3e61458b:1234567",
                0L,
                Arrays.asList("id", "ca"),
                ImmutableMap.of(
                        "id", 123,
                        "ca", 456,
                        "cb", 789
                )
        );
        String expectedPayloadTableRowKey = "85503b0c-ae28-473a-ae70-86af3e61458b:1234567";

        String result = HBaseRowKeyMapper.getPayloadTableHBaseRowKey(augmentedRow);

        Assert.assertEquals("Payload table key test", expectedPayloadTableRowKey, result);

    }

}
