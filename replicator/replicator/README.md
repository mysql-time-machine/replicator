## Binlog Flusher
Flushes MySQL database tables to the binlog in order to have the initial snapshot of the database in the binlog.

### Usage
***WARNING: you should NEVER run binlog-flusher on the MySQL master. Binlog flusher renames all tables during the blackhole copy and if the program does not finish successfully, the database can be left in an inconsistent state and in worst case you will need to reclone the database. The probability of this is low, but still, if it happens, you do NOT want this to happen on MySQL master.***

Assuming you adhere to the above warning, you can flush the contents of a database to the binlog with:

````
python data-flusher.py [--mycnf .my.cnf] [--db $db] [--table $table] [--stop-slave/--no-stop-slave] [--start-slave/--no-start-slave] --host $host [--skip $skip]
````
Where parameters are: 
- skip: separated list of schemas to skip (not to flush in the binlog)
- mycnf: filename that contains the admin privileges used for the blackhole_copy of initial snapshot.
- table: if you want to flush only the specific tables

````
[client]
user=admin
password=admin
````

Then start replication with

````
mysql> start slave;
````

In case binlog flusher didn't exit gracefully and the database has been left in an inconsistent state, you can run db-recovery.py to recover the database.

````
python db-recovery.py [--mycnf .my.cnf] [--db $db] [--table $table] [--stop-slave/--no-stop-slave] [--start-slave/--no-start-slave] --host $host --hashfile $hashfile [--skip $skip]
````

Where $hashfile contains the mappings from the backup table name to original table name.

````
_BKTB_1, $tablename1$
_BKTB_2, $tablename2$
....
````
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