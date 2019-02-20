package com.booking.replication.commons.services;

public class TagCombo {
    public final String mysqlReplicantTag;
    public final String mysqlActiveSchemaTag;
    public final String hbase;
    public final String kafkaTag;
    public String schemaRegistryTag;
    public final String zookeeperTag;

    public TagCombo(
        String mysqlReplicantTag,
        String mysqlActiveSchemaTag,
        String hbase,
        String kafkaTag,
        String zookeeperTag,
        String schemaRegistryTag
    ) {
        this.zookeeperTag = zookeeperTag;
        this.hbase = hbase;
        this.mysqlActiveSchemaTag = mysqlActiveSchemaTag;
        this.mysqlReplicantTag = mysqlReplicantTag;
        this.kafkaTag = kafkaTag;
        this.schemaRegistryTag = schemaRegistryTag;
    }
}
