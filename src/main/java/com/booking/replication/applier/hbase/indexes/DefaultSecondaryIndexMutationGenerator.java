package com.booking.replication.applier.hbase.indexes;

import com.booking.replication.Configuration;
import com.booking.replication.applier.hbase.PutMutation;
import com.booking.replication.applier.hbase.util.Salter;
import com.booking.replication.augmenter.AugmentedRow;
import com.booking.replication.schema.TableNameMapper;
import com.google.common.base.Joiner;
import org.apache.commons.collections.map.HashedMap;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
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

            LOGGER.info("processing secondary_index => " + secondaryIndexName);
            String secondaryIndexTableName = TableNameMapper.getSecondaryIndexTableName(
                configuration.getHbaseNamespace(),
                mySQLTableName, secondaryIndexName
            );
            LOGGER.info("\t table name will be => " + secondaryIndexTableName);

            List<String> orderedSecondaryIndexColumnNames =
                    configuration.getSecondaryIndexesForTable(mySQLTableName).get(secondaryIndexName).indexColumns;

            //Set<String> secondaryIndexColumnNames = new HashSet<>(orderedSecondaryIndexColumnNames);

            Long columnTimestamp = row.getEventV4Header().getTimestamp();

//            List<String> orderedSecondaryIndexColumnValues =
//                    getOrderedSecondaryIndexColumnValues(
//                            row,
//                            secondaryIndexColumnNames
//                    );

            SecondaryIndexOperationSpec secondaryIndexHBaseRowIDs = getSecondaryIndexOperation(
                    orderedSecondaryIndexColumnNames,
                row
            );

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

                    Put put = new Put(
                        Bytes.toBytes(
                            secondaryIndexHBaseRowIDs.getSecondaryIndexHBaseRowKeyBefore()
                        )
                    );
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
                    //  => check id the value of secondary index has changed in the primary row
                    //      => if there is no change then noop
                    //      => else:
                    //          => not remove the original secondary index entry
                    //          => it will update row status with 'DU' (which means that this
                    //             entry no longer points to original row)
                    //          => then it will insert another row with new value for secondary index
                    //
                    //          NOTE: the 'DU' here means that the value of secondary index column
                    //                actually changed. This is different from the 'row_status' for
                    //                primary rows where the 'U' means that one of the columns changes.
                    //                This allows fast indexed queries of type:
                    //
                    //                   "get all reservations which used to have
                    //                   checkin X, but they no longer do since they were modified"

                    // TODO: maybe rename 'row_status' here to 'index_status' to avoid ambiguity

                    boolean changed = false;
                    String skRowKeyBefore = secondaryIndexHBaseRowIDs.getSecondaryIndexHBaseRowKeyBefore();
                    String skRowKeyAfter  = secondaryIndexHBaseRowIDs.getSecondaryIndexHBaseRowKeyAfter();

                    if (!skRowKeyAfter.equals(skRowKeyBefore)) {
                        changed = true;
                    }

                    if (changed) {
                        // mark with 'DU' - deleted due to update
                        Put putOnRowKeyBeforeOp = new Put(Bytes.toBytes(skRowKeyBefore));
                        putOnRowKeyBeforeOp.addColumn(
                                CF,
                                Bytes.toBytes("row_status"),
                                columnTimestamp,
                                Bytes.toBytes("DU")
                        );
                        String rowUri = null; // no validator for secondary indexes
                        PutMutation mutationOnRowKeyBeforeOp =
                            new PutMutation(
                                    putOnRowKeyBeforeOp,
                                secondaryIndexTableName,
                                rowUri,
                                false,
                                false,
                                configuration
                            );
                        secondaryIndexMutations.add(mutationOnRowKeyBeforeOp);

                        // then insert new secondary key with valueAfter
                        Put putOnRowKeyAfterOp = new Put(Bytes.toBytes(skRowKeyAfter));
                        putOnRowKeyAfterOp.addColumn(
                                CF,
                                Bytes.toBytes("row_status"),
                                columnTimestamp,
                                Bytes.toBytes("I")
                        );
                        String rowUriAfter = null; // no validator for secondary indexes
                        PutMutation mutationOnRowKeyAfterOp =
                            new PutMutation(
                                putOnRowKeyAfterOp,
                                secondaryIndexTableName,
                                rowUriAfter,
                                false,
                                false,
                                configuration
                            );
                        secondaryIndexMutations.add(mutationOnRowKeyAfterOp);
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
                    Put put = new Put(
                        Bytes.toBytes(
                            secondaryIndexHBaseRowIDs.getSecondaryIndexHBaseRowKeyAfter()
                        )
                    );

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

//    private List<String> getOrderedSecondaryIndexColumnValues(
//            AugmentedRow row,
//            Set<String> secondaryIndexColumnNames) {
//        List<String> orderedSecondaryIndexColumnValues = new ArrayList<>();
//
//        TreeMap<Integer,String> skColumnsSortedByOP = new TreeMap<>();
//
//        for (String columnName : row.getEventColumns().keySet()) {
//
//            if (secondaryIndexColumnNames.contains(columnName)) {
//
//                String cVal = row.getEventColumns().get(columnName).toString();
//
//                int columnOrdinalPosition =
//                        row.getTableSchemaVersion().getColumnsSchema().get(columnName).getOrdinalPosition();
//
//                skColumnsSortedByOP.put(columnOrdinalPosition,cVal);
//            }
//
//        }
//        orderedSecondaryIndexColumnValues.addAll(skColumnsSortedByOP.values());
//
//        return orderedSecondaryIndexColumnValues;
//    }

    private SecondaryIndexOperationSpec getSecondaryIndexOperation (
            List<String> secondaryIndexColumnNames,
            AugmentedRow row
    ) {
        // This is sorted by column OP (from information schema)
        List<String> skColumnNames  = secondaryIndexColumnNames;
        Map<String,List<String>> skColumnValues = new HashedMap();

        String pkOP = new String();
        switch (row.getEventType()) {
            case "INSERT":
                pkOP = "INSERT";
                skColumnValues.put("INSERT", new ArrayList<>());
                break;
            case "DELETE":
                pkOP = "DELETE";
                skColumnValues.put("DELETE", new ArrayList<>());
                break;
            case "UPDATE":
                pkOP = "UPDATE";
                skColumnValues.put("UPDATE_BEFORE", new ArrayList<>());
                skColumnValues.put("UPDATE_AFTER", new ArrayList<>());
                break;
            default:
                LOGGER.error("Wrong event type. Expected RowType event.");
                // TODO: throw WrongEventTypeException
                break;
        }

        for (String skColumnName : skColumnNames) {

            Map<String, String> skCell = row.getEventColumns().get(skColumnName);

            switch (row.getEventType()) {
                case "INSERT":
                    skColumnValues.get("INSERT").add(skCell.get("value"));
                    break;
                case "DELETE":
                    skColumnValues.get("DELETE").add(skCell.get("value"));
                    break;
                case "UPDATE":
                    // for secondary index we need value before in order to update
                    // the key with 'DU' marker. Otherwise we would have
                    // a wrong secondary index.
                    skColumnValues.get("UPDATE_BEFORE").add(skCell.get("value_before"));
                    skColumnValues.get("UPDATE_AFTER").add(skCell.get("value_after"));
                    break;
                default:
                    LOGGER.error("Wrong event type. Expected RowType event.");
                    // TODO: throw WrongEventTypeException
                    break;
            }
        }

        String skHbaseRowID_Before = "";
        String skHbaseRowID_After  = "";

        String saltingPartOfKey;
        switch (row.getEventType()) {
            case "INSERT":
                skHbaseRowID_After = Joiner.on(";").join(skColumnValues.get("INSERT"));
                saltingPartOfKey = skColumnValues.get("INSERT").get(0);
                skHbaseRowID_After = Salter.saltRowKey(skHbaseRowID_After, saltingPartOfKey);

                LOGGER.info("skHbaseRowID_After => " + skHbaseRowID_After);

                break;
            case "DELETE":
                skHbaseRowID_Before = Joiner.on(";").join(skColumnValues.get("DELETE"));
                saltingPartOfKey = skColumnValues.get("DELETE").get(0);
                skHbaseRowID_Before = Salter.saltRowKey(skHbaseRowID_Before, saltingPartOfKey);

                LOGGER.info("skHbaseRowID_Before => " + skHbaseRowID_Before);

                break;
            case "UPDATE":
                skHbaseRowID_Before = Joiner.on(";").join(skColumnValues.get("UPDATE_BEFORE"));
                saltingPartOfKey = skColumnValues.get("UPDATE_BEFORE").get(0);
                skHbaseRowID_Before = Salter.saltRowKey(skHbaseRowID_Before, saltingPartOfKey);
                skHbaseRowID_After = Joiner.on(";").join(skColumnValues.get("UPDATE_AFTER"));
                saltingPartOfKey = skColumnValues.get("UPDATE_AFTER").get(0);
                skHbaseRowID_After = Salter.saltRowKey(skHbaseRowID_After, saltingPartOfKey);

                LOGGER.info("skHbaseRowID_Before => " + skHbaseRowID_Before);
                LOGGER.info("skHbaseRowID_After => " + skHbaseRowID_After);

                break;
            default:
                LOGGER.error("Wrong event type. Expected RowType event.");
                // TODO: throw WrongEventTypeException
                break;
        }

        return new SecondaryIndexOperationSpec(pkOP, skHbaseRowID_Before, skHbaseRowID_After);

    }
}
