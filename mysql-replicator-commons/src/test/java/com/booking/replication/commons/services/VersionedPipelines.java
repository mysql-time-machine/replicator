package com.booking.replication.commons.services;

public final class VersionedPipelines {

     public static final TagCombo defaultTags = new TagCombo(
            "mysql:5.6.38",
            "mysql:5.6.38",
            "harisekhon/hbase-dev:1.3",
            "wurstmeister/kafka:2.11-1.1.1",
            "zookeeper:latest",
             "confluentinc/cp-schema-registry:4.0.1"
     );

     public static final TagCombo[] versionCombos = {
             new TagCombo(
                     "mysql:5.6.38",
                     "mysql:5.6.38",
                     "harisekhon/hbase-dev:1.3",
                     "wurstmeister/kafka:latest",
                     "zookeeper:latest",
                     "confluentinc/cp-schema-registry:latest"
             ),
             new TagCombo(
                     "mysql:5.7.22",
                     "mysql:5.7.22",
                     "harisekhon/hbase-dev:1.3",
                     "wurstmeister/kafka:latest",
                     "zookeeper:latest",
                     "confluentinc/cp-schema-registry:latest"
             ),
             new TagCombo(
                     "mysql:8.0.11",
                     "mysql:8.0.11",
                     "harisekhon/hbase-dev:1.3",
                     "wurstmeister/kafka:latest",
                     "zookeeper:latest",
                     "confluentinc/cp-schema-registry:latest"
             )
    };
}
