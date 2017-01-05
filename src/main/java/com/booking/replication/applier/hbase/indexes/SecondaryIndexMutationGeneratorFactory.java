package com.booking.replication.applier.hbase.indexes;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by bosko on 12/29/16.
 */
public class SecondaryIndexMutationGeneratorFactory {

    private static final Logger LOGGER = LoggerFactory.getLogger(SecondaryIndexMutationGeneratorFactory.class);

    public static SecondaryIndexMutationGenerator getSecondaryIndexMutationGenerator(String indexType) {

        if (indexType.equals("SIMPLE_HISTORICAL")) {
            return new DefaultSecondaryIndexMutationGenerator();
        }
        else {
            LOGGER.error("Unsupported secondary index type: " + indexType);
            return null;
        }
    }
}
