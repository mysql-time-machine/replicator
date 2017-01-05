package com.booking.replication.applier.hbase.indexes;
import com.booking.replication.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import  java.util.HashMap;

/**
 * Created by bosko on 1/5/17.
 */
public class SecondaryIndexMutationGenerators {

    private static final Logger LOGGER = LoggerFactory.getLogger(SecondaryIndexMutationGenerators.class);

    private final HashMap<String, SecondaryIndexMutationGenerator> generators;

    private final Configuration configuration;

    public SecondaryIndexMutationGenerator getSecondaryInexMutationGenerator(String indexType) throws Exception {
        if (generators.containsKey(indexType)) {
            return generators.get(indexType);
        }
        else {
            throw new Exception("Unsupported secondary index type " + indexType);
        }
    }

    public SecondaryIndexMutationGenerators(Configuration configuration) {
        this.configuration = configuration;
        generators = new HashMap<>();

        initGenerators(configuration);
    }

    private void initGenerators(Configuration configuration) {

        for (String mySQLTableName: configuration.indexesByTable.keySet()) {
            for (String secondaryIndexName: configuration.getSecondaryIndexesForTable(mySQLTableName).keySet()) {

                String indexType =
                    configuration.getSecondaryIndexesForTable(mySQLTableName).get(secondaryIndexName).indexType;
                try {
                    SecondaryIndexMutationGenerator secondaryIndexMutationGenerator =
                            SecondaryIndexMutationGeneratorFactory.getSecondaryIndexMutationGenerator(indexType);
                    generators.put(indexType, secondaryIndexMutationGenerator);
                }
                catch (Exception e){
                    LOGGER.error("Error while trying to acquire mutation generator for indexType " + indexType, e);
                }
            }
        }
    }
}
