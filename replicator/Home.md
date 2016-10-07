# MySQL Time Machine
Collection of services and tools for creating, processing and storing streams of MySQL data changes.

# Status
Testing, beta-level quality.

# Components:

## 1. Binlog Flusher
Flushes MySQL database tables to the binlog in order to have the initial snapshot of the database in the binlog.

## 2. MySQL to HBase Replicator.
Replicates data changes from MySQL binlog to HBase, preserving the previous data versions. Intended
for auditing purposes of historical data. In addition can maintain special daily-changes tables which
are convenient for fast and cheap imports from HBase to Hive.


## 3. HBaseSnapshotter
HBaseSnapshotter is a Spark application that takes a snapshot of an HBase table at a given point in time and store it to a Hive table. Usually you can export from HBase to Hive but you can only get the latest version, as Hive doesn't have enough flexibility to access different versions of an HBase table. Spark framework allows this flexibility since it has the ability and the API to access and manipulate both HBase and Hive.
