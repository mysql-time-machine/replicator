package com.booking.replication.applier.kafka;

import com.booking.replication.Configuration;
import com.booking.replication.util.CaseInsensitiveMap;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.jruby.RubyProcess;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.booking.replication.applier.kafka.Util.getHashCode_HashCustomColumn;
import static com.booking.replication.applier.kafka.Util.getHashCode_HashPrimaryKeyValuesMethod;
import static org.junit.Assert.*;

/**
 * Created by bosko on 11/19/17.
 */
public class UtilTest {

    @Test
    public void testGetHashCode_HashPrimaryColumnsMethod() throws Exception {

        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());

        // TODO: move to separate configuration tests
        String config = "replication_schema:\n" +
                "    name:      'test'\n" +
                "    username:  '__USER__'\n" +
                "    password:  '__PASS__'\n" +
                "    host_pool: ['localhost2', 'localhost']\n" +
                "\n" +
                "metadata_store:\n" +
                "    username: '__USER__'\n" +
                "    password: '__PASS__'\n" +
                "    host:     'localhost'\n" +
                "    database: 'test_active_schema'\n" +
                "    file:\n" +
                "        path: '/opt/replicator/replicator_metadata'\n" +
                "kafka:\n" +
                "    broker: \"kafka-1:9092,kafka-2:9092,kafka-3:9092,kafka-4:9092\"\n" +
                "    topic:  test\n" +
                "    tables: [\"sometable\"]\n" +
                "\n" +
                "    partitioning_method: 2\n" +
                "    partition_columns:\n" +
                "        TestTable: TestColumn\n" +
                "mysql_failover:\n" +
                "    pgtid:\n" +
                "        p_gtid_pattern: '(?<=_pseudo_gtid_hint__asc\\:)(.{8}\\:.{16}\\:.{8})'\n" +
                "        p_gtid_prefix: \"use `pgtid_meta`;\"\n";

        InputStream in = new ByteArrayInputStream(config.getBytes(StandardCharsets.UTF_8.name()));
        Configuration configuration = mapper.readValue(in, Configuration.class);

        int partitioningMethod = configuration.getKafkaPartitioningMethod();
        assertTrue(partitioningMethod == Configuration.PARTITIONING_METHOD_HASH_PRIMARY_COLUMN);

        List<String> pkColumns = new ArrayList<>();
        pkColumns.add("id");

        Map<String,Map<String,String>> eventColumns = new CaseInsensitiveMap<>();

        eventColumns.put("TestColumn", new HashMap<>());
        eventColumns.get("TestColumn").put("value_before", "ABC");
        eventColumns.get("TestColumn").put("value_after", "BCD");
        eventColumns.get("TestColumn").put("type", "VARCHAR");

        eventColumns.put("id", new HashMap<>());
        eventColumns.get("id").put("value_before", "123");
        eventColumns.get("id").put("value_after", "123");
        eventColumns.get("id").put("type", "INT");

        String eventType = "UPDATE";

        int hashCode = getHashCode_HashPrimaryKeyValuesMethod(
                eventType, pkColumns, eventColumns
        );
        assertTrue(hashCode == 48690);
    }

    @Test
    public void testGetHashCode_HashCustomColumn() throws Exception {

        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());

        // TODO: move to separate configuration tests
        String config = "replication_schema:\n" +
                "    name:      'test'\n" +
                "    username:  '__USER__'\n" +
                "    password:  '__PASS__'\n" +
                "    host_pool: ['localhost2', 'localhost']\n" +
                "\n" +
                "metadata_store:\n" +
                "    username: '__USER__'\n" +
                "    password: '__PASS__'\n" +
                "    host:     'localhost'\n" +
                "    database: 'test_active_schema'\n" +
                "    file:\n" +
                "        path: '/opt/replicator/replicator_metadata'\n" +
                "kafka:\n" +
                "    broker: \"kafka-1:9092,kafka-2:9092,kafka-3:9092,kafka-4:9092\"\n" +
                "    topic:  test\n" +
                "    tables: [\"sometable\"]\n" +
                "\n" +
                "    partitioning_method: 3\n" +
                "    partition_columns:\n" +
                "        TestTable: TestColumn\n" +
                "mysql_failover:\n" +
                "    pgtid:\n" +
                "        p_gtid_pattern: '(?<=_pseudo_gtid_hint__asc\\:)(.{8}\\:.{16}\\:.{8})'\n" +
                "        p_gtid_prefix: \"use `pgtid_meta`;\"\n";

        InputStream in = new ByteArrayInputStream(config.getBytes(StandardCharsets.UTF_8.name()));
        Configuration configuration = mapper.readValue(in, Configuration.class);

        int partitioningMethod = configuration.getKafkaPartitioningMethod();
        assertTrue(partitioningMethod == Configuration.PARTITIONING_METHOD_HASH_CUSTOM_COLUMN);

        HashMap<String, String> partitionColumns = configuration.getKafkaPartitionColumns();
        assertTrue(partitionColumns.get("TestTable").equals("TestColumn"));

        List<String> pkColumns = new ArrayList<>();
        pkColumns.add("id");

        Map<String,Map<String,String>> eventColumns = new CaseInsensitiveMap<>();

        eventColumns.put("TestColumn", new HashMap<>());
        eventColumns.get("TestColumn").put("value", "XYZ");
        eventColumns.get("TestColumn").put("type", "VARCHAR");

        eventColumns.put("id", new HashMap<>());
        eventColumns.get("id").put("value", "789");
        eventColumns.get("id").put("type", "INT");

        String eventType = "INSERT";

        int hashCode = getHashCode_HashCustomColumn(
                eventType, "TestTable", eventColumns, partitionColumns
        );
        assertTrue(hashCode == 87417);
    }
}