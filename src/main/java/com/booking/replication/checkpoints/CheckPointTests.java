package com.booking.replication.checkpoints;

import com.booking.replication.metrics.Metric;
import com.booking.replication.metrics.ReplicatorMetrics;
import com.booking.replication.util.MutableLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.util.HashMap;

/**
 * Created by bosko on 3/29/16.
 */
public class CheckPointTests {

    private final com.booking.replication.Configuration replicatorConfiguration;
    private final ReplicatorMetrics replicatorMetrics;

    private static final Logger LOGGER = LoggerFactory.getLogger(CheckPointTests.class);

    public CheckPointTests(com.booking.replication.Configuration configuration, ReplicatorMetrics replicatorMetrics) {
        replicatorConfiguration = configuration;
        this.replicatorMetrics  = replicatorMetrics;
    }

    public boolean verifyConsistentCountersOnRotateEvent(BigInteger hbaseTotalRowsCommitted, BigInteger mysqlTotalRowsProccessed) {

        LOGGER.info("===============================================================");
        LOGGER.info("==================== CHECKPOINT TESTS =========================");
        boolean isDelta = replicatorConfiguration.isWriteRecentChangesToDeltaTables();

        final int TESTS_TO_RUN = 2;

        int testsPassed = 0;

        if (isDelta) {

            // TESTS:
            //
            //    (hbaseTotalRowsCommitted - mysqlTotalRowsProcessed) == SUM_PER_DELTA_TABLE(hbaseRowsCommitted)
            //
            //    mysqlTotalRowsProcessed == SUM_PER_MIRRORED_TABLE(hbaseRowsCommitted)

            BigInteger sumOfRowsCommitedForDeltaTables    = BigInteger.ZERO;
            BigInteger sumOfRowsCommitedForMirroredTables = BigInteger.ZERO;

            if (replicatorMetrics.getTotalsPerTable() != null) {
                for (String tableName : replicatorMetrics.getTotalsPerTable().keySet()) {

                    HashMap<Integer, MutableLong> tableTotals = replicatorMetrics.getTotalsPerTable().get(tableName);

                    if (tableName.contains(":")) {
                        if (tableName.contains("delta:")) { // <- convention: all delta tables must go in 'delta' namespace
                            LOGGER.info("HBase Delta Table: " + tableName);

                            if (tableTotals != null) {
                                for (Integer metricID : tableTotals.keySet()) {
                                    if (tableTotals.get(metricID) != null) {
                                        LOGGER.info("\t [" +
                                                tableName +
                                                "." +
                                                Metric.getCounterName(metricID) +
                                                "]" +
                                                " => " +
                                                tableTotals.get(metricID).getValue());

                                        if (metricID == Metric.TOTAL_HBASE_ROWS_AFFECTED) {
                                            BigInteger deltaCommitedForTable = BigInteger.valueOf(tableTotals.get(Metric.TOTAL_HBASE_ROWS_AFFECTED).getValue());
                                            sumOfRowsCommitedForDeltaTables = sumOfRowsCommitedForDeltaTables.add(deltaCommitedForTable);
                                        }
                                    }
                                }
                            }
                            else {
                                LOGGER.info("No count for table => " + tableName);
                            }
                        }
                        else {
                            LOGGER.info("HBase Mirrored Table: " + tableName);

                            if (tableTotals != null) {
                                for (Integer metricID : tableTotals.keySet()) {
                                    if (tableTotals.get(metricID) != null) {
                                        LOGGER.info("\t [" +
                                                tableName +
                                                "." +
                                                Metric.getCounterName(metricID) +
                                                "]" +
                                                " => " +
                                                tableTotals.get(metricID).getValue());

                                        if (metricID == Metric.TOTAL_HBASE_ROWS_AFFECTED) {
                                            BigInteger mirroredCommitedForTable = BigInteger.valueOf(tableTotals.get(Metric.TOTAL_HBASE_ROWS_AFFECTED).getValue());
                                            sumOfRowsCommitedForMirroredTables = sumOfRowsCommitedForMirroredTables.add(mirroredCommitedForTable);
                                        }
                                    }
                                    else {
                                        LOGGER.info("No writes for this table were made");
                                    }
                                }
                            }
                            else {
                                LOGGER.info("No count for table => " + tableName);
                            }
                        }
                    }
                    else {
                        LOGGER.info("MySQL source table");
                    }
                }
            }


            LOGGER.info("Running check point test:" +
                    " \n     (hbaseTotalRowsCommitted - mysqlTotalRowsProcessed)== SUM_PER_DELTA_TABLE(hbaseRowsCommitted)");

            LOGGER.info("(" + hbaseTotalRowsCommitted + " - " + mysqlTotalRowsProccessed + ") == " + sumOfRowsCommitedForDeltaTables);

            if ((hbaseTotalRowsCommitted.subtract(mysqlTotalRowsProccessed)).equals(sumOfRowsCommitedForDeltaTables)) {
                LOGGER.info("PASS");
                testsPassed++;
            }
            else {
                LOGGER.info("FAIL");
            }

            LOGGER.info("Running check point test:" +
                    " \n     mysqlTotalRowsProcessed == SUM_PER_MIRRORED_TABLE(hbaseRowsCommitted)");

            LOGGER.info(mysqlTotalRowsProccessed + " == " + sumOfRowsCommitedForMirroredTables);

            if (mysqlTotalRowsProccessed.equals(sumOfRowsCommitedForMirroredTables)) {
                LOGGER.info("PASS");
                testsPassed++;
            }
            else {
                LOGGER.info("FAIL");
            }

            if (testsPassed == TESTS_TO_RUN) {
                return true;
            }
            else {
                return false;
            }
        }
        else {
            // 1 MySQL row ==> 1 HBase Row
            LOGGER.info("Running check point test:" +
                        "\n    " + "mysqlTotalRowsProccessed" + " == " + "hbaseTotalRowsCommitted");
            LOGGER.info("\n    " +  mysqlTotalRowsProccessed  + " == " +  hbaseTotalRowsCommitted);


            if (mysqlTotalRowsProccessed == hbaseTotalRowsCommitted) {
                LOGGER.info("PASS");
                return true;
            }
            else {
                LOGGER.info("FAIL");
                return false;
            }
        }
    }
}
