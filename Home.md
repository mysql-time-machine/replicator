# MySQL Time Machine
Collection of services and tools for creating, processing and storing streams of MySQL data changes.

# Status
Testing, beta-level quality.

# Components:

## 1. Binlog Flusher
Flushes MySQL database tables to the binlog in order to have the initial snapshot of the database in the binlog.

### 1.1 Usage
Flush database to binlog with:
````
python data-flusher.py --mycnf .my.cnf --host $host [--db $db] [--table $table]
````
Where .my.cnf contains the admin privileges used for the blackhole_copy of initial snapshot.
````
[client]
user=admin
password=admin
````
Then start replication with
````
mysql> start slave;
````

## 2. MySQL to HBase Replicator.
Replicates data changes from MySQL binlog to HBase, preserving the previous data versions. Intended
for auditing purposes of historical data. In addition can maintain special daily-changes tables which
are convenient for fast and cheap imports from HBase to Hive.

### 2.1 Usage
#### 2.1.1. Replicate initial binlog snapshot to hbase
````
java -jar hbrepl-0.9.9-3.jar --hbase-namespace $hbase-namespace --applier hbase --schema $schema --binlog-filename $first-binlog-filename --config-path $config-path [--shard $shard] --initial-snapshot
````

#### 2.1.2 Replication after initial snapshot
````
java -jar hbrepl-0.9.9-3.jar --hbase-namespace $hbase-namespace --applier hbase --schema $schema --binlog-filename $binlog-filename --config-path $config-path [--shard $shard] [--delta]
````

#### 2.1.3 Replicate range of binlog files and output db events as JSON to STDOUT:
````
java -jar hbrepl-0.9.9-3.jar --applier STDOUT --schema $schema --binlog-filename $binlog-filename --last-binlog-filename $last-binlog-filename-to-process --config-path $config-path 
````

#### 2.2 Configuration file structure:
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
graphite:
    url:       'graphite_host[:<graphite_port>]'
    namespace: 'no-stats'
hive_imports:
    tables: ['sometable']
````

## 3. HBaseSnapshotter

### 3.1 Overview
HBaseSnapshotter is a Spark application that takes a snapshot of an HBase table at a given point in time and store it to a Hive table.
Usually you can export from HBase to Hive but you can only get the latest version, as Hive doesn't have enough flexibility
to access different versions of an HBase table. Spark framework allows this flexibility since it has the ability and the API
to access and manipulate both HBase and Hive.

### 3.2 Configuration
HBaseSnapshotter needs a yaml configuration file to be provided through the option
```
--config <CONFIG-PATH>
```

The format of the yaml config is as follows:
```
hbase:
    zookeeper_quorum:  ['hbase-zk1-host', 'hbase-zkN-host']
    schema: ['family1:qualifier1', 'familyN:qualifierN']
hive:
    default_null: 'DEFAULT NULL VALUE'
```

*zookeeper_quorum*: A list of HBase zookeeper nodes to establish a connection with an HBase table.<br/>
*schema*: A list of columns forming the schema of the source HBase table. A column is formatted as *'Familyname:Qualifiername'*.<br/> 
*default_null*: The default value to be inserted in Hive table, if the corresponding column in HBase is missing in a specific row.
Since HBase is a key-value store, not all columns need to exist in every row. if *default_null* is not configured, the default
null value will be "NULL".

To write a configuration file, you can start by copying the file config-default.yml and customise it to your own needs.

### 3.3 Hive Schema
The resulted Hive table will have the same schema as the source HBase table, but the column names will be formatted
as *'Familyname_Qualifiername'*. A new column will be added to the Hive table named *"k_hbase_key"* for storing the HBase key
of this row. For now, the columns in Hive will be of type *string* only, but in the future you might be able to provide the
types in the config file.

### 3.4 Usage:
```
hbase-snapshotter [options] <source table> <dest table>

Options:
  --pit <TIMESTAMP>
        Takes a snapshot of the latest HBase version available before the given timestamp (exclusive). If this option is not specified, the latest timestamp will be used.
  --config <CONFIG-PATH>
        The path of a yaml config file.
  --help
        Prints this usage text

Arguments:
  <source table>
        The source HBase table you are copying from. It should be in the format NAMESPACE:TABLENAME
  <dest table>
        The destination Hive table you are copying to. It should be in the format DATABASE.TABLENAME
```
### 3.5 Build
````
1. First you need to build a fat jar containing all the dependencies needed by this app. Inside the project's folder, execute the command:
    sbt assembly

   If you don't have sbt-assembly installed, take a look at this https://github.com/sbt/sbt-assembly
   This will build a fat jar at this path: target/scala-2.10/HBaseSnapshotter-assembly-1.0.jar


2. You can then copy this jar along with the files hbase-snapshotter and config-default.yml to a docker container or a hadoop box supporting Spark:
    scp target/scala-2.10/HBaseSnapshotter-assembly-1.0.jar hadoop-box.example.com:~
    scp hbase-snapshotter hadoop-box.example.com:~
    scp config-default.yml hadoop-box.example.com:~
    
    Replace hadoop-box.example.com by the actual name of your hadoop box.

3. Provide your config settings in the file config-default.yml, or in a new yaml file.

4. Finally, from the docker or hadoop box, you can run the spark app via the bash script
    ~/hbase-snapshotter [options] <source table> <dest table>
````
