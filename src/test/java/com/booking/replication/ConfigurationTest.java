package com.booking.replication;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ConfigurationTest {

    @Test
    public void testConverterConfigStringifyJsonNull() throws Exception {

        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());

        String config =
                "replication_schema:\n" +
                "    name:      'test'\n" +
                "    username:  '__USER__'\n" +
                "    password:  '__PASS__'\n" +
                "    host_pool: ['localhost2', 'localhost']\n" +
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
                "converter:\n" +
                "    stringify_null: 1\n" +
                "mysql_failover:\n" +
                "    pgtid:\n" +
                "        p_gtid_pattern: '(?<=_pseudo_gtid_hint__asc\\:)(.{8}\\:.{16}\\:.{8})'\n" +
                "        p_gtid_prefix: \"use `pgtid_meta`;\"\n";

        InputStream in = new ByteArrayInputStream(config.getBytes(StandardCharsets.UTF_8.name()));
        Configuration configuration = mapper.readValue(in, Configuration.class);

        boolean convertNullToString = configuration.getConverterStringifyNull();
        assertTrue(convertNullToString);
    }

    @Test
    public void testConverterConfigDoNotStringifyJsonNull() throws Exception {

        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());

        String config =
                "replication_schema:\n" +
                "    name:      'test'\n" +
                "    username:  '__USER__'\n" +
                "    password:  '__PASS__'\n" +
                "    host_pool: ['localhost2', 'localhost']\n" +
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
                "converter:\n" +
                "    stringify_null: 0\n" +
                "mysql_failover:\n" +
                "    pgtid:\n" +
                "        p_gtid_pattern: '(?<=_pseudo_gtid_hint__asc\\:)(.{8}\\:.{16}\\:.{8})'\n" +
                "        p_gtid_prefix: \"use `pgtid_meta`;\"\n";

        InputStream in = new ByteArrayInputStream(config.getBytes(StandardCharsets.UTF_8.name()));
        Configuration configuration = mapper.readValue(in, Configuration.class);

        boolean convertNullToString = configuration.getConverterStringifyNull();
        assertFalse(convertNullToString);
    }

    @Test
    public void testKafkaConfigNumberOfRowsPerMessage() throws Exception {

        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());

        String config =
                "replication_schema:\n" +
                "    name:      'test'\n" +
                "    username:  '__USER__'\n" +
                "    password:  '__PASS__'\n" +
                "    host_pool: ['localhost2', 'localhost']\n" +
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
                "    rows_per_message: 5\n" +
                "converter:\n" +
                "    stringify_null: 1\n" +
                "mysql_failover:\n" +
                "    pgtid:\n" +
                "        p_gtid_pattern: '(?<=_pseudo_gtid_hint__asc\\:)(.{8}\\:.{16}\\:.{8})'\n" +
                "        p_gtid_prefix: \"use `pgtid_meta`;\"\n";

        InputStream in = new ByteArrayInputStream(config.getBytes(StandardCharsets.UTF_8.name()));
        Configuration configuration = mapper.readValue(in, Configuration.class);

        int numberOfRowsPerMessage = configuration.getKafkaNumberOfRowsPerMessage();
        assertEquals(5, numberOfRowsPerMessage);
    }

    @Test
    public void testKafkaConfigDefaultNumberOfRowsPerMessage() throws Exception {

        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());

        String config =
                "replication_schema:\n" +
                "    name:      'test'\n" +
                "    username:  '__USER__'\n" +
                "    password:  '__PASS__'\n" +
                "    host_pool: ['localhost2', 'localhost']\n" +
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
                "converter:\n" +
                "    stringify_null: 1\n" +
                "mysql_failover:\n" +
                "    pgtid:\n" +
                "        p_gtid_pattern: '(?<=_pseudo_gtid_hint__asc\\:)(.{8}\\:.{16}\\:.{8})'\n" +
                "        p_gtid_prefix: \"use `pgtid_meta`;\"\n";

        InputStream in = new ByteArrayInputStream(config.getBytes(StandardCharsets.UTF_8.name()));
        Configuration configuration = mapper.readValue(in, Configuration.class);

        int numberOfRowsPerMessage = configuration.getKafkaNumberOfRowsPerMessage();
        assertEquals(10, numberOfRowsPerMessage);
    }

    @Test
    public void testPipelineOrchestratorConfigCustomRewind() throws Exception {

        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());

        String config =
                "replication_schema:\n" +
                "    name:      'test'\n" +
                "    username:  '__USER__'\n" +
                "    password:  '__PASS__'\n" +
                "    host_pool: ['localhost2', 'localhost']\n" +
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
                "orchestrator:\n" +
                "    rewinding_threshold: 499\n" +
                "    rewinding_enabled: true\n" +
                "converter:\n" +
                "    stringify_null: 1\n" +
                "mysql_failover:\n" +
                "    pgtid:\n" +
                "        p_gtid_pattern: '(?<=_pseudo_gtid_hint__asc\\:)(.{8}\\:.{16}\\:.{8})'\n" +
                "        p_gtid_prefix: \"use `pgtid_meta`;\"\n";

        InputStream in = new ByteArrayInputStream(config.getBytes(StandardCharsets.UTF_8.name()));
        Configuration configuration = mapper.readValue(in, Configuration.class);

        boolean rewindingEnabled = configuration.orchestratorConfiguration.isRewindingEnabled();
        long rewindingThreshold = configuration.orchestratorConfiguration.getRewindingThreshold();

        assertEquals(true, rewindingEnabled);
        assertEquals(499, rewindingThreshold);
    }

    @Test
    public void testPipelineOrchestratorConfigDefaultRewind() throws Exception {

        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());

        String config =
                "replication_schema:\n" +
                        "    name:      'test'\n" +
                        "    username:  '__USER__'\n" +
                        "    password:  '__PASS__'\n" +
                        "    host_pool: ['localhost2', 'localhost']\n" +
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
                        "converter:\n" +
                        "    stringify_null: 1\n" +
                        "mysql_failover:\n" +
                        "    pgtid:\n" +
                        "        p_gtid_pattern: '(?<=_pseudo_gtid_hint__asc\\:)(.{8}\\:.{16}\\:.{8})'\n" +
                        "        p_gtid_prefix: \"use `pgtid_meta`;\"\n";

        InputStream in = new ByteArrayInputStream(config.getBytes(StandardCharsets.UTF_8.name()));
        Configuration configuration = mapper.readValue(in, Configuration.class);

        boolean rewindingEnabled = configuration.orchestratorConfiguration.isRewindingEnabled();
        long rewindingThreshold = configuration.orchestratorConfiguration.getRewindingThreshold();

        assertEquals(true, rewindingEnabled);
        assertEquals(1000, rewindingThreshold);
    }

    @Test
    public void testPipelineOrchestratorConfigRewindOff() throws Exception {

        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());

        String config =
                "replication_schema:\n" +
                        "    name:      'test'\n" +
                        "    username:  '__USER__'\n" +
                        "    password:  '__PASS__'\n" +
                        "    host_pool: ['localhost2', 'localhost']\n" +
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
                        "orchestrator:\n" +
                        "    rewinding_enabled: false\n" +
                        "converter:\n" +
                        "    stringify_null: 1\n" +
                        "mysql_failover:\n" +
                        "    pgtid:\n" +
                        "        p_gtid_pattern: '(?<=_pseudo_gtid_hint__asc\\:)(.{8}\\:.{16}\\:.{8})'\n" +
                        "        p_gtid_prefix: \"use `pgtid_meta`;\"\n";

        InputStream in = new ByteArrayInputStream(config.getBytes(StandardCharsets.UTF_8.name()));
        Configuration configuration = mapper.readValue(in, Configuration.class);

        boolean rewindingEnabled = configuration.orchestratorConfiguration.isRewindingEnabled();

        assertEquals(false, rewindingEnabled);
    }

    @Test
    public void testHBaseConfigurationUseSnappyOff() throws Exception {

        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());

        String config =
                "replication_schema:\n" +
                        "    name:      'test'\n" +
                        "    username:  '__USER__'\n" +
                        "    password:  '__PASS__'\n" +
                        "    host_pool: ['localhost2', 'localhost']\n" +

                        "metadata_store:\n" +
                        "    username: '__USER__'\n" +
                        "    password: '__PASS__'\n" +
                        "    host:     'localhost'\n" +
                        "    database: 'test_active_schema'\n" +
                        "    file:\n" +
                        "        path: '/opt/replicator/replicator_metadata'\n" +

                        "hbase:\n" +
                        "    namespace: \"test\"\n" +
                        "    zookeeper_quorum: [\"hbase\"]\n" +
                        "    use_snappy: false\n" +
                        "    hive_imports:\n" +
                        "        tables: ['test1','test2','test3']\n" +

                        "orchestrator:\n" +
                        "    rewinding_enabled: false\n" +
                        "converter:\n" +
                        "    stringify_null: 1\n" +
                        "mysql_failover:\n" +
                        "    pgtid:\n" +
                        "        p_gtid_pattern: '(?<=_pseudo_gtid_hint__asc\\:)(.{8}\\:.{16}\\:.{8})'\n" +
                        "        p_gtid_prefix: \"use `pgtid_meta`;\"\n";

        InputStream in = new ByteArrayInputStream(config.getBytes(StandardCharsets.UTF_8.name()));
        Configuration configuration = mapper.readValue(in, Configuration.class);

        boolean useSnappy = configuration.useSnappyForHBaseTables();

        assertEquals(false, useSnappy);
    }

    @Test
    public void testHBaseConfigurationUseSnappyDefault() throws Exception {

        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());

        String config =
                "replication_schema:\n" +
                        "    name:      'test'\n" +
                        "    username:  '__USER__'\n" +
                        "    password:  '__PASS__'\n" +
                        "    host_pool: ['localhost2', 'localhost']\n" +

                        "metadata_store:\n" +
                        "    username: '__USER__'\n" +
                        "    password: '__PASS__'\n" +
                        "    host:     'localhost'\n" +
                        "    database: 'test_active_schema'\n" +
                        "    file:\n" +
                        "        path: '/opt/replicator/replicator_metadata'\n" +

                        "hbase:\n" +
                        "    namespace: \"test\"\n" +
                        "    zookeeper_quorum: [\"hbase\"]\n" +
                        "    hive_imports:\n" +
                        "        tables: ['test1','test2','test3']\n" +

                        "orchestrator:\n" +
                        "    rewinding_enabled: false\n" +
                        "converter:\n" +
                        "    stringify_null: 1\n" +
                        "mysql_failover:\n" +
                        "    pgtid:\n" +
                        "        p_gtid_pattern: '(?<=_pseudo_gtid_hint__asc\\:)(.{8}\\:.{16}\\:.{8})'\n" +
                        "        p_gtid_prefix: \"use `pgtid_meta`;\"\n";

        InputStream in = new ByteArrayInputStream(config.getBytes(StandardCharsets.UTF_8.name()));
        Configuration configuration = mapper.readValue(in, Configuration.class);

        boolean useSnappy = configuration.useSnappyForHBaseTables();

        assertEquals(true, useSnappy);
    }

    @Test
    public void testHBaseConfigurationSkipTables() throws Exception {

        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());

        String config =
                "replication_schema:\n" +
                        "    name:      'test'\n" +
                        "    username:  '__USER__'\n" +
                        "    password:  '__PASS__'\n" +
                        "    host_pool: ['localhost2', 'localhost']\n" +

                        "metadata_store:\n" +
                        "    username: '__USER__'\n" +
                        "    password: '__PASS__'\n" +
                        "    host:     'localhost'\n" +
                        "    database: 'test_active_schema'\n" +
                        "    file:\n" +
                        "        path: '/opt/replicator/replicator_metadata'\n" +

                        "hbase:\n" +
                        "    namespace: \"test\"\n" +
                        "    zookeeper_quorum: [\"hbase\"]\n" +
                        "    use_snappy: false\n" +
                        "    hive_imports:\n" +
                        "        tables: ['test1','test2','test3']\n" +
                        "    skip_tables:\n" +
                        "        skip_list: ['skip1','skip2','skip3']\n" +

                        "orchestrator:\n" +
                        "    rewinding_enabled: false\n" +
                        "converter:\n" +
                        "    stringify_null: 1\n" +
                        "mysql_failover:\n" +
                        "    pgtid:\n" +
                        "        p_gtid_pattern: '(?<=_pseudo_gtid_hint__asc\\:)(.{8}\\:.{16}\\:.{8})'\n" +
                        "        p_gtid_prefix: \"use `pgtid_meta`;\"\n";

        InputStream in = new ByteArrayInputStream(config.getBytes(StandardCharsets.UTF_8.name()));
        Configuration configuration = mapper.readValue(in, Configuration.class);

        List<String> tablesToSkip = configuration.getTablesToSkip();

        List<String> expected = Arrays.asList("skip1", "skip2", "skip3");

        for (int i = 0; i < 3; i++) {
            assertEquals(expected.get(i), tablesToSkip.get(i));
        }
    }
}