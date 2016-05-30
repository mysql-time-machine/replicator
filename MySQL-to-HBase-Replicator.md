## MySQL to HBase Replicator.
Replicates data changes from MySQL binlog to HBase, preserving the previous data versions. Intended
for auditing purposes of historical data. In addition can maintain special daily-changes tables which
are convenient for fast and cheap imports from HBase to Hive.

### Usage
#### Replicate initial binlog snapshot to hbase
````
java -jar hbrepl-0.9.9-3.jar --hbase-namespace $hbase-namespace --applier hbase --schema $schema --binlog-filename $first-binlog-filename --config-path $config-path [--shard $shard] --initial-snapshot
````

#### Replication after initial snapshot
````
java -jar hbrepl-0.9.9-3.jar --hbase-namespace $hbase-namespace --applier hbase --schema $schema --binlog-filename $binlog-filename --config-path $config-path [--shard $shard] [--delta]
````

#### Replicate range of binlog files and output db events as JSON to STDOUT:
````
java -jar hbrepl-0.9.9-3.jar --applier STDOUT --schema $schema --binlog-filename $binlog-filename --last-binlog-filename $last-binlog-filename-to-process --config-path $config-path 
````

#### Configuration file structure:
````
replication_schema:
    name:     'replicated_schema_name'
    username: 'user'
    password: 'pass'
    slaves:   ['localhost', 'localhost']
metadata_store:
    username: 'user'
    password: 'pass'
    host:     'active_schema_host'
    database: 'active_schema_database'
hbase:
    namespace: 'schema_namespace'
    zookeeper_quorum:  ['hbase-zk1-host', 'hbase-zkN-host']
    hive_imports:
        tables: ['sometable']
graphite:
    url:       'graphite_host[:<graphite_port>]'
    namespace: 'no-stats'
````