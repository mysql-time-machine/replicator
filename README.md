[![][Build Status img]][Build Status]
[![][Coverage Status img]][Coverage Status]
[![][Known Vulnerabilities img]][Known Vulnerabilities]
[![][license img]][license]

## MySQL Replicator
Replicates data changes from MySQL binlog to HBase or Kafka. In case of HBase, preserves the previous data versions. HBase storage is intended for auditing and analysis of historical data. In addition, special daily-changes tables can be maintained in HBase, which are convenient for fast and cheap imports from HBase to Hive. Replication to Kafka is intended for easy real-time access to a stream of data changes.

## Intro
This readme file provides basic introduction on how to get started. For more details, refer to official documentation at [mysql-time-machine](https://mysql-time-machine.github.io/).

### Building required Docker images
1. Run `mvn clean package` from the root of the `replicator` repository to build the MySQL Replicator distribution that will be used later;
2. Copy the distribution file to the `images/replicator_testing/input/replicator/` directory inside the `docker` repository;
3. Run `container_build.sh` script from the `images/replicator_testing/` directory inside the `docker` repository;
4. Run `docker images` to verify that `replicator-testing` image has been built successfully;

### Getting Started with MySQL Replicator
Replicator assumes that there is a pre-installed environment in which it can run. This environment consists of:

 - MySQL Instance
 - Zookeeper Instance
 - Graphite Instance (or none in case of console metrics reporter)
 - Target Datastore Instance (Kafka, HBase, or none in case of STDOUT)

Easiest way to test drive the replicator is to use docker to locally create this needed environment. In addition to docker you will need [docker-compose](https://docs.docker.com/compose/) installed locally.

````
git clone https://github.com/mysql-time-machine/docker.git
cd docker/docker-compose/replicator_testing
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

This folder contains:
1. different utility scripts to setup and run the replicator
2. replicator configuration file
3. log configuration file

There are the steps to initialize the environemnt and start the replication with latest 0.16 version:

````
 ./00_setup   # will unznip the jars from distribution
 ./01_seed    # will initialize mysql with random data
 ./02_run_016 # will start the replication
````

Random data in the seed step has been inserted in pre-created database 'test' in precreated table 'sometable'. The provided mysql instance is configured to use RBR and binlogs are active.

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

As the replication is running, you can observe the replication statistics at graphite dashboard: http://localhost/dashboard/

### PACKAGING

Packaging the project is really simple with Maven

```
mvn clean package
```

Will generate a tar.gz distribution as output. The distribution will contain all dependencies.


### DEPLOY

To deploy a new version to Maven central it's enough executing

```
mvn clean deploy -P release
```

If previous step didn't work is probably because you don't have a SonaType account or a published GPG key. Follow these steps:

1. [Create a Sonatype Account](https://issues.sonatype.org/secure/Signup!default.jspa)
2. [Create a PGP Signature](http://central.sonatype.org/pages/working-with-pgp-signatures.html)

Now you should be in conditions to deploy the project.

### AUTHOR
Bosko Devetak <bosko.devetak@gmail.com>

### CONTRIBUTORS

Ashwin Konale<a href="https://github.com/akonale">[akonale]</a>

Carlos Tasada <a href="https://github.com/ctasada">[ctasada]</a>

Dmitrii Tcyganov <a href="https://github.com/dtcyganov">[dtcyganov]</a>

Evgeny Dmitriev <a href="https://github.com/dmitrieveu">[dmitrieveu]</a>

Fabricio Damasceno <a href="https://github.com/forlando">[forlando]</a>

Gaurav Kohli <a href="https://github.com/gauravkohli">[gauravkohli]</a>

Greg Franklin <a href="https://github.com/gregf1">[gregf1]</a>

Islam Hassan <a href="https://github.com/ishassan">[ishassan]</a>

Mikhail Dutikov <a href="https://github.com/mikhaildutikov">[mikhaildutikov]</a>

Muhammad Abbady <a href="https://github.com/muhammad-abbady">[muhammad-abbady]</a>

Philippe Bruhat (BooK) <a href="https://github.com/book">[book]</a>

Pavel Salimov <a href="https://github.com/chcat">[chcat]</a>

Pedro Silva <a href="https://github.com/pedros">[pedros]</a>

Raynald Chung <a href="https://github.com/raynald">[raynald]</a>

Rares Mirica <a href="https://github.com/mrares">[mrares]</a>

Steve Aurigema <a href="https://github.com/steveauri">[steveauri]</a>

### ACKNOWLEDGMENT
Replicator was originally developed for Booking.com. With approval from Booking.com, the code and specification were generalized and published as Open Source on github, for which the author would like to express his gratitude.

### COPYRIGHT AND LICENSE
Copyright (C) 2015, 2016, 2017, 2018, 2019 by Author and Contributors

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
[Known Vulnerabilities img]:https://snyk.io/test/github/mysql-time-machine/replicator/badge.svg
[Known Vulnerabilities]:https://snyk.io/test/github/mysql-time-machine/replicator
[license]:LICENSE
[license img]:https://img.shields.io/badge/license-Apache%202-blue.svg
