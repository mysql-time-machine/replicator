[![][Build Status img]][Build Status]
[![][Coverage Status img]][Coverage Status]
[![][license img]][license]
[![FOSSA Status](https://app.fossa.io/api/projects/git%2Bgithub.com%2Fmysql-time-machine%2Freplicator.svg?type=shield)](https://app.fossa.io/projects/git%2Bgithub.com%2Fmysql-time-machine%2Freplicator?ref=badge_shield)
[![][Maven Central img]][Maven Central]
[![][Javadocs img]][Javadocs]

## MySQL Replicator
Replicates data changes from MySQL binlog to HBase or Kafka. In case of HBase, preserves the previous data versions. HBase storage is intended for auditing and analysis of historical data. In addition, special daily-changes tables can be maintained in HBase, which are convenient for fast and cheap imports from HBase to Hive. Replication to Kafka is intended for easy real-time access to a stream of data changes.

### Documentation
This readme file provides some basic documentation on how to get started. For more details, refer to official documentation at [mysql-time-machine](https://mysql-time-machine.github.io/).

### Getting Started with MySQL Replicator
Replicator assumes that there is a preinstalled environment in which it can run. This environment consists of:

 - MySQL Instance
 - Zookeeper Instance
 - Graphite Instance
 - Target Store Instance (Kafka, HBase, or none in case of STDOUT)
 
Easiest way to test drive the replicator is to use docker to locally create this needed environment. In addition to docker you will need [docker-compose](https://docs.docker.com/compose/) installed locally.

````
git clone https://github.com/mysql-time-machine/docker.git
cd docker/docker-compose/replicator_kafka
````

Start all containers (mysql, kafka, graphite, replicator, zookeeper)
 
```
  ./run_all
```

Now, in another terminal, you can connect to the replicator container
 
```` 
 ./attach_to_replicator
 cd /replicator
````
 
 This folder contains the replicator jar, the replicator configuration file, log configuration and some utility scripts. 
 Now we can insert some random data in mysql:
 
 ````
 ./random_mysql_ops
 ...
 ('TwIPn','4216871','313785','NIrnXGEpqJI gGDstvhs'),
 ('AwqgI','4831311','930233','IHwkTOuEnOqGdEWNzJtq'),
 ('WIJCB','1516599','487420','rPnOHfZlIvEEvFFEIGiW'),
 ...
 ````

 This data has been inserted in pre-created database 'test' in precreated table 'sometable'. The provided mysql instance is configured to use RBR and binlogs are active.
 
````
  mysql --host=mysql --user=root --pass=mysqlPass
  
  mysql> use test;
  mysql> show tables;
  +----------------+
  | Tables_in_test |
  +----------------+
  | sometable      |
  +----------------+
  1 row in set (0.00 sec)
````
 
 Now we can replicate the binlog content to Kafka. 
 
````
 ./run_kafka
````
 
 And read the data from Kafka
 
 ````
 ./read_kafka
````

In this example we have writen rows to mysql, then replicated the binlogs to kafka and then red from Kafka sequentially. However, these processes can be run in parallel as the real life setup would work.

As the replication is running, you can observe the replication statisticts at graphite dashboard: http://localhost/dashboard/

### AUTHOR
Bosko Devetak <bosko.devetak@gmail.com>

### CONTRIBUTORS
Carlos Tasada <a href="https://github.com/raynald">[ctasada]</a>

Dmitrii Tcyganov <a href="https://github.com/dtcyganov">[dtcyganov]</a>

Evgeny Dmitriev <a href="https://github.com/dmitrieveu">[dmitrieveu]</a>

Greg Franklin <a href="https://github.com/gregf1">[gregf1]</a>

Islam Hassan <a href="https://github.com/ishassan">[ishassan]</a>

Mikhail Dutikov <a href="https://github.com/mikhaildutikov">[mikhaildutikov]</a>

Muhammad Abbady <a href="https://github.com/muhammad-abbady">[muhammad-abbady]</a>

Philippe Bruhat (BooK) <a href="https://github.com/book">[book]</a>

Pavel Salimov <a href="https://github.com/chcat">[chcat]</a>

Pedro Silva <a href="https://github.com/pedros">[pedros]</a>

Raynald Chung <a href="https://github.com/raynald">[raynald]</a>

Rares Mirica <a href="https://github.com/mrares">[mrares]</a>

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


[![FOSSA Status](https://app.fossa.io/api/projects/git%2Bgithub.com%2Fmysql-time-machine%2Freplicator.svg?type=large)](https://app.fossa.io/projects/git%2Bgithub.com%2Fmysql-time-machine%2Freplicator?ref=badge_large)
