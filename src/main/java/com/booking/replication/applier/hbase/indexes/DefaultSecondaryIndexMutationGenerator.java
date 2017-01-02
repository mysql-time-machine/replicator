package com.booking.replication.applier.hbase.indexes;

import com.booking.replication.Configuration;
import com.booking.replication.applier.hbase.PutMutation;
import com.booking.replication.augmenter.AugmentedRow;
import com.booking.replication.schema.TableNameMapper;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

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
                    configuration.getSecondaryIndexesForTable(mySQLTableName).get(secondaryIndexName).indexColumns;

            Set<String> secondaryIndexColumnNames = new HashSet<>(orderedSecondaryIndexColumnNames);

            Long columnTimestamp = row.getEventV4Header().getTimestamp();

            List<String> orderedSecondaryIndexColumnValues =
                    getOrderedSecondaryIndexColumnValues(
                            row,
                            secondaryIndexColumnNames
                    );

            // TODO: !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
            String secondaryIndexHBaseRowID = getSecondaryIndexHBaseRowKey(
                    orderedSecondaryIndexColumnValues,
                    row
            ); // TODO <- implement key build !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

            switch (row.getEventType()) {
                case "DELETE": {

                    // In case of DELETE primary row remains in HBase history. The question
                    // is weather we want to have secondary index pointing to that row
                    // or not. That depends on the use case. If we want to get all the
                    // rows to which this index pointed in history that is one case. If
                    // we need the rows which are currently available that is different
                    // use case. Depending on the use case we can implement different
                    // types of secondary indexes. The 'SIMPLE_HISTORICAL' type works as follows:
                    //
                    //  => it will not delete the secondary index row
                    //  => it will update the row status with 'D' marker
                    //  => the number of versions in secondary index table is 1

                    Put put = new Put(Bytes.toBytes(secondaryIndexHBaseRowID));
                    put.addColumn(
                            CF,
                            Bytes.toBytes("row_status"),
                            columnTimestamp,
                            Bytes.toBytes("D") // primary row was deleted, so this index points to deleted row
                    );
                    String rowUri = null; // no validator for secondary indexes
                    PutMutation mutation =
                            new PutMutation(put, secondaryIndexTableName, rowUri, false, false, configuration);
                    secondaryIndexMutations.add(mutation);

                    break;
                }
                case "UPDATE": {

                    // since before update there was an insert in some point in
                    // time, this means that this id is allready indexed in
                    // secondary index. However, when secondary index column is updated
                    // the effect is similar to a delete. The original row has a different
                    // value for the indexed column and that means that the previous
                    // value of secondary index column no longer points to the original row.
                    // For historical analysis it may be relevant to know that this value
                    // used to point to original row. The 'SIMPLE_HISTORICAL' index type will:
                    //
                    //  => not remove the secondary index entry
                    //  => it will update row status with 'DU' (which means that this
                    //     entry no longer points to original row)

                    // NOTE: the 'DU' here means that the value of secondary index column
                    //       actually changed. This is different from the 'row_status' for
                    //       primary rows where the 'U' means that one of the columns changes.
                    //       This allows fast indexed queries of type:
                    //
                    //          "get all reservations which used to have
                    //          checkin X, but they no longer do since they were modified"

                    // TODO: maybe rename 'row_status' here to 'index_status' to avoid ambiguity

                    // Determine if the value_before != value_after for the index column since
                    // in that case the index no longer points to primary row. Otherwise
                    // its a no-op for index_status

                    boolean changed = false;
                    for (String columnName : secondaryIndexColumnNames) {
                        if (!row.getValueBeforeFromColumnName(columnName).equals(row.getValueAfterFromColumnName(columnName))) {
                            changed = true;
                        }
                    }
                    if (changed) {
                        // mark with 'DU' - deleted due to update
                        Put put = new Put(Bytes.toBytes(secondaryIndexHBaseRowID));
                        put.addColumn(
                                CF,
                                Bytes.toBytes("row_status"),
                                columnTimestamp,
                                Bytes.toBytes("DU")
                        );
                        String rowUri = null; // no validator for secondary indexes
                        PutMutation mutation =
                                new PutMutation(put, secondaryIndexTableName, rowUri, false, false, configuration);
                        secondaryIndexMutations.add(mutation);
                    }
                    else {
                        // noop
                    }

                    break;
                }
                case "INSERT": {

                    // For 'SIMPLE_HISTORICAL' secondary index type, in case of insert, the rowKey
                    // allready contains all information so the only column used is
                    // the row status where the 'I' marker is added
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

    private List<String> getOrderedSecondaryIndexColumnValues(
            AugmentedRow row,
            Set<String> secondaryIndexColumnNames) {
        List<String> orderedSecondaryIndexColumnValues = new ArrayList<>();

        TreeMap<Integer,String> skColumnsSortedByOP = new TreeMap<>();

        for (String columnName : row.getEventColumns().keySet()) {

            if (secondaryIndexColumnNames.contains(columnName)) {

                String cVal = row.getEventColumns().get(columnName).toString();

                int columnOrdinalPosition =
                        row.getTableSchemaVersion().getColumnsSchema().get(columnName).getOrdinalPosition();

                skColumnsSortedByOP.put(columnOrdinalPosition,cVal);
            }

        }
        orderedSecondaryIndexColumnValues.addAll(skColumnsSortedByOP.values());

        return orderedSecondaryIndexColumnValues;
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
