package com.booking.replication.applier.hbase.indexes;

import com.booking.replication.Configuration;
import com.booking.replication.applier.hbase.PutMutation;
import com.booking.replication.augmenter.AugmentedRow;
import com.booking.replication.schema.TableNameMapper;
import com.booking.replication.schema.column.ColumnSchema;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Created by bosko on 12/29/16.
 */
public class DefaultSecondaryIndexMutationGenerator implements SecondaryIndexMutationGenerator {

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultSecondaryIndexMutationGenerator.class);

    private static final byte[] CF                           = Bytes.toBytes("d");

    @Override
    public List<PutMutation> getPutsForSecondaryIndexes(
            Configuration configuration,
            AugmentedRow row
        ) {

        List<PutMutation> secondaryIndexMutations = new ArrayList<>();

        String  mySQLTableName    = row.getTableName();

        // loop all secondary indexes
        for (String secondaryIndexName: configuration.getSecondaryIndexesForTable(mySQLTableName).keySet()) {
            String secondaryIndexTableName = TableNameMapper.getSecondaryIndexTableName(
                configuration.getHbaseNamespace(),
                mySQLTableName, secondaryIndexName
            );

            List<String> orderedSecondaryIndexColumnNames =
                    configuration.getSecondaryIndexesForTable(mySQLTableName).get(secondaryIndexName);

            Set<String> secondaryIndexColumnNames = new HashSet<>(orderedSecondaryIndexColumnNames);

            switch (row.getEventType()) {
                case "DELETE": {

                    // in case of DELETE primary row remains in HBase and therefore
                    // the secondary index should remain, so no action is done
                    // on secondary index.

                    break;
                }
                case "UPDATE": {

                    // since before update there was an insert in some point in
                    // time, this means that this id is allready indexed in
                    // secondary index. So, no action is done on secondary index.

                    break;
                }
                case "INSERT": {

                    Long columnTimestamp = row.getEventV4Header().getTimestamp();

                    TreeMap<Integer,String> skColumnsSortedByOP = new TreeMap<>();
                    List<String> orderedSecondaryIndexColumnValues = new ArrayList<>();

                    for (String columnName : row.getEventColumns().keySet()) {

                        if (secondaryIndexColumnNames.contains(columnName)) {

                            String cVal = row.getEventColumns().get(columnName).toString();

                            int columnOrdinalPosition =
                                    row.getTableSchemaVersion().getColumnsSchema().get(columnName).getOrdinalPosition();

                            skColumnsSortedByOP.put(columnOrdinalPosition,cVal);
                        }

                    }
                    orderedSecondaryIndexColumnValues.addAll(skColumnsSortedByOP.values());

                    // TODO: !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
                    String secondaryIndexHBaseRowID = getSecondaryIndexHBaseRowKey(
                            orderedSecondaryIndexColumnValues,
                            row
                    ); //TODO <- !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

                    Put put = new Put(Bytes.toBytes(secondaryIndexHBaseRowID));

                    // for simple secondary index we only update row status column since all other
                    // information is in the rowKey itself
                    put.addColumn(
                            CF,
                            Bytes.toBytes("row_status"),
                            columnTimestamp,
                            Bytes.toBytes("I")
                    );

                    String rowUri = null; // no validator for secondary indexes
                    PutMutation mutation =
                            new PutMutation(put, secondaryIndexTableName, rowUri,false, false, configuration);

                    secondaryIndexMutations.add(mutation);

                    break;
                }
                default:
                    LOGGER.error("ERROR: Wrong event type. Expected RowType event. Shutting down...");
                    System.exit(1);

            } // end switch

        } // next secondary index

        return secondaryIndexMutations;
    }

    private String getSecondaryIndexHBaseRowKey(
            List<String> secondaryIndexOrderedColumnValues,
            AugmentedRow row
        ) {

        // TODO
        // should be: join(";",@secondary_index_column_values) + ";$primary_key"
        return null;
    }
}
