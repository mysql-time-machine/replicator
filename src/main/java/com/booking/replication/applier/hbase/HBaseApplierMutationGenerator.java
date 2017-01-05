package com.booking.replication.applier.hbase;

import com.booking.replication.applier.hbase.indexes.SecondaryIndexMutationGenerator;
import com.booking.replication.applier.hbase.indexes.SecondaryIndexMutationGenerators;
import com.booking.replication.applier.hbase.util.RowKeyGenerator;
import com.booking.replication.augmenter.AugmentedRow;
import com.booking.replication.schema.TableNameMapper;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Created by bosko on 4/18/16.
 */
public class HBaseApplierMutationGenerator {

    // TODO: add other index types
    private final String SECONDARY_INDEX_TYPE = "SIMPLE_HISTORICAL";

    private static final byte[] CF                           = Bytes.toBytes("d");

    private final com.booking.replication.Configuration configuration;

    private static final Logger LOGGER = LoggerFactory.getLogger(HBaseApplierMutationGenerator.class);

    private final SecondaryIndexMutationGenerators secondaryIndexMutationGenerators;
    // Constructor
    public HBaseApplierMutationGenerator(com.booking.replication.Configuration configuration) {
        this.configuration = configuration;

        secondaryIndexMutationGenerators = new SecondaryIndexMutationGenerators(configuration);
     }

    /**
     * Transforms a list of {@link AugmentedRow} to a list of hbase mutations
     *
     * @param augmentedRows a list of augmented rows
     * @return a list of HBase mutations
     */
    public List<PutMutation> generateMutations(List<AugmentedRow> augmentedRows) {

        Set<String> tablesForDelta = configuration.getTablesForWhichToTrackDailyChanges().stream().collect(Collectors.toSet());

        boolean writeDelta = configuration.isWriteRecentChangesToDeltaTables();

        return augmentedRows.stream()
                .flatMap( row -> getExpandedMutationStream(tablesForDelta, writeDelta, row) )
                .collect(Collectors.toList());

    }

    private Stream<PutMutation> getExpandedMutationStream(Set<String> tablesForDelta, boolean writeDelta, AugmentedRow row) {

        boolean addDeltaToStream = false;
        boolean addSecondaryIndexesToStream = false;

        if (writeDelta && tablesForDelta.contains(row.getTableName())) {
            addDeltaToStream = true;
        }
        if (configuration.getSecondaryIndexesForTable(row.getTableName()).size() > 0) {
            addSecondaryIndexesToStream = true;
        }

        // construct mutation stream
        List<PutMutation> mutationsToSend = new ArrayList<>();

        mutationsToSend.add(getPutForMirroredTable(row));

        if (addDeltaToStream) {
            mutationsToSend.add( getPutForDeltaTable(row));
        }

        if (addSecondaryIndexesToStream) {
            try {
                List<PutMutation> rowSecondaryIndexesPutMutations = getPutsForSecondaryIndexes(row);
                if (rowSecondaryIndexesPutMutations != null && rowSecondaryIndexesPutMutations.size() > 0) {
                    for (PutMutation secondaryMutation : rowSecondaryIndexesPutMutations) {
                        mutationsToSend.add(secondaryMutation);
                    }
                }
                else {
                    LOGGER.error("Missing secondary index mutations");
                }
            }
            catch (Exception e) {
                LOGGER.error("Error while generating mutations for secondary indexes.", e);
            }
        }

        return mutationsToSend.stream();
    }

    private List<PutMutation> getPutsForSecondaryIndexes(AugmentedRow row)  {

        String tableName = row.getTableName();

        List<PutMutation> secondaryIndexMutations  = new ArrayList<>();

        if (configuration.getSecondaryIndexesForTable(tableName) != null) {

            for (String secondaryIndexName: configuration.getSecondaryIndexesForTable(tableName).keySet()) {

                String indexType =  configuration.getSecondaryIndexesForTable(tableName).get(secondaryIndexName).indexType;

                try {
                    SecondaryIndexMutationGenerator secondaryIndexMutationGenerator =
                            secondaryIndexMutationGenerators.getSecondaryInexMutationGenerator(indexType);

                    List<PutMutation> secondaryIndexPutMutations = secondaryIndexMutationGenerator.getPutsForSecondaryIndex(
                            configuration,
                            row,
                            secondaryIndexName
                    );

                    secondaryIndexMutations.addAll(secondaryIndexPutMutations);
                }
                catch (Exception e) {
                    LOGGER.error("Error while trying to acquire mutation generator for inex type " + indexType);
                }
            }
        }

        return secondaryIndexMutations;
    }

    private PutMutation getPutForMirroredTable(AugmentedRow row) {

        // RowID
        String hbaseRowID = RowKeyGenerator.getSaltedHBaseRowKey(row);

        String hbaseTableName =
                configuration.getHbaseNamespace() + ":" + row.getTableName().toLowerCase();

        Put put = new Put(Bytes.toBytes(hbaseRowID));

        switch (row.getEventType()) {
            case "DELETE": {

                // No need to process columns on DELETE. Only write delete marker.
                Long columnTimestamp = row.getEventV4Header().getTimestamp();
                String columnName = "row_status";
                String columnValue = "D";
                put.addColumn(
                        CF,
                        Bytes.toBytes(columnName),
                        columnTimestamp,
                        Bytes.toBytes(columnValue)
                );
                break;
            }
            case "UPDATE": {

                // Only write values that have changed
                Long columnTimestamp = row.getEventV4Header().getTimestamp();
                String columnValue;

                for (String columnName : row.getEventColumns().keySet()) {

                    String valueBefore = row.getEventColumns().get(columnName).get("value_before");
                    String valueAfter = row.getEventColumns().get(columnName).get("value_after");

                    if ((valueAfter == null) && (valueBefore == null)) {
                        // no change, skip;
                    } else if (
                            ((valueBefore == null) && (valueAfter != null))
                                    ||
                                    ((valueBefore != null) && (valueAfter == null))
                                    ||
                                    (!valueAfter.equals(valueBefore))) {

                        columnValue = valueAfter;
                        put.addColumn(
                                CF,
                                Bytes.toBytes(columnName),
                                columnTimestamp,
                                Bytes.toBytes(columnValue)
                        );
                    } else {
                        // no change, skip
                    }
                }

                put.addColumn(
                        CF,
                        Bytes.toBytes("row_status"),
                        columnTimestamp,
                        Bytes.toBytes("U")
                );
                break;
            }
            case "INSERT": {

                Long columnTimestamp = row.getEventV4Header().getTimestamp();
                String columnValue;

                for (String columnName : row.getEventColumns().keySet()) {

                    columnValue = row.getEventColumns().get(columnName).get("value");
                    if (columnValue == null) {
                        columnValue = "NULL";
                    }

                    put.addColumn(
                            CF,
                            Bytes.toBytes(columnName),
                            columnTimestamp,
                            Bytes.toBytes(columnValue)
                    );
                }


                put.addColumn(
                        CF,
                        Bytes.toBytes("row_status"),
                        columnTimestamp,
                        Bytes.toBytes("I")
                );
                break;
            }
            default:
                LOGGER.error("ERROR: Wrong event type. Expected RowType event. Shutting down...");
                System.exit(1);
        }

        return new PutMutation(put,hbaseTableName,getRowUri(row), true, false,configuration);

    }

    private PutMutation getPutForDeltaTable(AugmentedRow row) {

        String hbaseRowID = RowKeyGenerator.getSaltedHBaseRowKey(row);

        // String  replicantSchema   = configuration.getReplicantSchemaName().toLowerCase();
        String  mySQLTableName    = row.getTableName();
        Long    timestampMicroSec = row.getEventV4Header().getTimestamp();
        boolean isInitialSnapshot = configuration.isInitialSnapshotMode();

        String deltaTableName = TableNameMapper.getCurrentDeltaTableName(
                timestampMicroSec,
                configuration.getHbaseNamespace(),
                mySQLTableName,
                isInitialSnapshot
        );

        Put put = new Put(Bytes.toBytes(hbaseRowID));

        switch (row.getEventType()) {
            case "DELETE": {

                // For delta tables in case of DELETE, just write a delete marker

                Long columnTimestamp = row.getEventV4Header().getTimestamp();
                String columnName = "row_status";
                String columnValue = "D";
                put.addColumn(
                        CF,
                        Bytes.toBytes(columnName),
                        columnTimestamp,
                        Bytes.toBytes(columnValue)
                );
                break;
            }
            case "UPDATE": {

                // for delta tables write the latest version of the entire row

                Long columnTimestamp = row.getEventV4Header().getTimestamp();

                for (String columnName : row.getEventColumns().keySet()) {
                    put.addColumn(
                            CF,
                            Bytes.toBytes(columnName),
                            columnTimestamp,
                            Bytes.toBytes(row.getEventColumns().get(columnName).get("value_after"))
                    );
                }

                put.addColumn(
                        CF,
                        Bytes.toBytes("row_status"),
                        columnTimestamp,
                        Bytes.toBytes("U")
                );
                break;
            }
            case "INSERT": {

                Long columnTimestamp = row.getEventV4Header().getTimestamp();
                String columnValue;

                for (String columnName : row.getEventColumns().keySet()) {

                    columnValue = row.getEventColumns().get(columnName).get("value");
                    if (columnValue == null) {
                        columnValue = "NULL";
                    }

                    put.addColumn(
                            CF,
                            Bytes.toBytes(columnName),
                            columnTimestamp,
                            Bytes.toBytes(columnValue)
                    );
                }

                put.addColumn(
                        CF,
                        Bytes.toBytes("row_status"),
                        columnTimestamp,
                        Bytes.toBytes("I")
                );
                break;
            }
            default:
                LOGGER.error("ERROR: Wrong event type. Expected RowType event. Shutting down...");
                System.exit(1);
        }

        return new PutMutation(put,deltaTableName,getRowUri(row),false, false,configuration);

    }

    private String getRowUri(AugmentedRow row){

        if (configuration.validationConfig == null) return null;

        // TODO: generate URI in a better way

        String eventType = row.getEventType();

        String table = row.getTableName();

        String keys  = row.getPrimaryKeyColumns().stream()
                .map( column -> {
                    try {

                        String value = row.getEventColumns().get(column).get( "UPDATE".equals(eventType) ? "value_after" : "value" );

                        return URLEncoder.encode(column,"UTF-8") + "=" + URLEncoder.encode(value,"UTF-8");

                    } catch (UnsupportedEncodingException e) {

                        LOGGER.error("Unexpected encoding exception", e);

                        return null;

                    }
                } )
                .collect(Collectors.joining("&"));

        return String.format("mysql://%s/%s?%s", configuration.validationConfig.getSourceDomain(), table, keys  );
    }
}
