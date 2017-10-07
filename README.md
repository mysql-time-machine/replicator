[![][Build Status img]][Build Status]
[![][Coverage Status img]][Coverage Status]
[![][license img]][license]
[![][Maven Central img]][Maven Central]
[![][Javadocs img]][Javadocs]
## MySQL Replicator
Replicates data changes from MySQL binlog to HBase or Kafka. In case of HBase, preserves the previous data versions. HBase storage is intended for auditing purposes of historical data. In addition, special daily-changes tables can be maintained in HBase, which are convenient for fast and cheap imports from HBase to Hive. Replication to Kafka is intended for easy real-time access to a stream of data changes.

### Documentation
This readme file provides some basic documentation. For more details, refer to official documentation at [mysql-time-machine](https://mysql-time-machine.github.io/).

### Usage

#### Replicate to STDOUT
````
java -jar mysql-replicator.jar \
    --applier STDOUT \
    --schema $schema \
    --binlog-filename $binlog-filename \
    --last-binlog-filename $last-binlog-filename-to-process \
    --config-path $config-path
````

#### Replicate to HBase
Initial snapshot (after the database has been flushed to the binlog with [binlog flusher](https://github.com/mysql-time-machine/replicator/tree/master/binlog-flusher):
````
java -jar mysql-replicator.jar \
    --hbase-namespace $hbase-namespace \
    --applier hbase --schema $schema \
    --binlog-filename $first-binlog-filename \
    --config-path $config-path \
    --initial-snapshot
````
After intiall snapshot:
````
java -jar mysql-replicator.jar \
    --hbase-namespace $hbase-namespace \
    --applier hbase \
    --schema $schema \
    --binlog-filename $binlog-filename \
    --config-path $config-path  \
    [--delta]
````

#### Replicate to Kafka
````
java -jar mysql-replicator.jar \
    --applier kafka \
    --schema $schema \
    --binlog-filename $binlog-filename \
    --config-path $config-path
````

#### Configuration file structure
Replicator configuration is contained in a single YAML file. The structure of the file with all supported options is:
````
replication_schema:
    name:      'replicated_schema_name'
    username:  'user'
    password:  'pass'
    host_pool: ['localhost']

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

# only one applier is needed (HBase or Kafka). If none is specified, the STDOUT is used
kafka:
    broker: "kafka-broker-1:port,...,kafka-broken-N:port"
    topic:  topic_name
    # tables to replicate to kafka, can be either a list of tables,
    # or an exlcussion filter
    tables: ["table_1", ..., "table_N"]
    excludetables: ["exlude_pattern_1",..., "exclude_pattern_N"]
hbase:
    namespace: 'schema_namespace'
    zookeeper_quorum:  ['hbase-zk1-host', 'hbase-zkN-host']
    hive_imports:
        tables: ['sometable']

# mysql-failover is optional
mysql_failover:
    pgtid:
        p_gtid_pattern: $regex_pattern_to_extract_pgtid
        p_gtid_prefix: $prefix_to_add_to_pgtid_query_used_in_orchestrator_url
    # orchestator is optional
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

### AUTHOR
Bosko Devetak <bosko.devetak@gmail.com>

### CONTRIBUTORS
Greg Franklin <a href="https://github.com/gregf1">gregf1</a>

Islam Hassan <a href="https://github.com/ishassan">ishassan</a>

Mikhail Dutikov <a href="https://github.com/mikhaildutikov">mikhaildutikov</a>

Pavel Salimov <a href="https://github.com/chcat">chcat</a>

Pedro Silva <a href="https://github.com/pedros">pedros</a>

Rares Mirica <a href="https://github.com/mrares">mrares</a>

Raynald Chung <a href="https://github.com/raynald">raynald</a>

### ACKNOWLEDGMENT
Replicator was originally developed for Booking.com. With approval from Booking.com, the code and specification were generalized and published as Open Source on github, for which the author would like to express his gratitude.

### COPYRIGHT AND LICENSE
Copyright (C) 2015, 2016, 2017 by Bosko Devetak

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

[Build Status]:https://travis-ci.org/mysql-time-machine/replicator
[Build Status img]:https://travis-ci.org/mysql-time-machine/replicator.svg?branch=master
[Coverage Status]:https://codecov.io/gh/mysql-time-machine/replicator
[Coverage Status img]:https://codecov.io/gh/mysql-time-machine/replicator/branch/master/graph/badge.svg
[Maven Central]:https://maven-badges.herokuapp.com/maven-central/com.booking/mysql-replicator
[Maven Central img]:https://maven-badges.herokuapp.com/maven-central/com.booking/mysql-replicator/badge.svg
[license]:LICENSE
[license img]:https://img.shields.io/badge/license-Apache%202-blue.svg
[Javadocs]:http://javadoc.io/doc/com.booking/mysql-replicator
[Javadocs img]:http://javadoc.io/badge/com.booking/mysql-replicator.svg
