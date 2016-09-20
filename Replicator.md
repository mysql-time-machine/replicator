## Replicator
Replicates data changes from MySQL binlog to HBase or Kafka. In case of HBase, preserves the previous data versions. HBase storage is intended for auditing purposes of historical data. In addition, special daily-changes tables can be maintained in HBase, which are convenient for fast and cheap imports from HBase to Hive. Replication to Kafka is intended for easy real-time access to a stream of data changes. 

### Usage
#### Replicate initial binlog snapshot to HBase
````
java -jar hbrepl-0.11.0.jar --hbase-namespace $hbase-namespace --applier hbase --schema $schema --binlog-filename $first-binlog-filename --config-path $config-path --initial-snapshot
````

#### Replication to HBase after initial snapshot
````
java -jar hbrepl-0.11.0.jar --hbase-namespace $hbase-namespace --applier hbase --schema $schema --binlog-filename $binlog-filename --config-path $config-path  [--delta]
````

#### Replication to Kafka
````
java -jar hbrepl-0.11.0.jar --applier kafka --schema $schema --binlog-filename $binlog-filename --config-path $config-path 
````

#### Replicate range of binlog files and output db events as JSON to STDOUT
````
java -jar hbrepl-0.11.0.jar --applier STDOUT --schema $schema --binlog-filename $binlog-filename --last-binlog-filename $last-binlog-filename-to-process --config-path $config-path 
````
#### Configuration file structure
````
replication_schema:
    name:     'replicated_schema_name'
    username: 'user'
    password: 'pass'
    host:     'localhost'
metadata_store:
    username: 'user'
    password: 'pass'
    host:     'active_schema_host'
    database: 'active_schema_database'
    # The following are options for storing replicator metadata, only one should be used (zookeeper or file)
    zookeeper:
        quorum: ['zk-host1', 'zk-host2']
        path: '/path/in/zookeeper'
    file:
        path: '/path/on/disk'
hbase:
    namespace: 'schema_namespace'
    zookeeper_quorum:  ['hbase-zk1-host', 'hbase-zkN-host']
    hive_imports:
        tables: ['sometable']
mysql_failover:
    pgtid:
        p_gtid_pattern: $regex_pattern_to_extract_pgtid
        p_gtid_prefix: $prefix_to_add_to_pgtid_query_used_in_orchestrator_url
    orchestrator:
        username: orchestrator-user-name
        password: orchestrator-password
        url:      http://orchestrator-host/api
metrics:
    frequency: 10 seconds
    reporters:
      graphite:
        namespace: 'graphite.namespace.prefix'
        url: 'graphite_host[:<graphite_port (default is 3002)>]'
# Optionally you can specify a console reporter for ease of testing
#      console:
#        timeZone: UTC
#        output: stdout
````