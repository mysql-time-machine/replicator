package com.booking.replication.audit;

import com.booking.replication.Configuration;
import com.booking.replication.Replicator;
import com.booking.replication.metrics.Metric;
import com.booking.replication.metrics.ReplicatorMetrics;
import com.booking.replication.util.MutableLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    public boolean verifyConsistentCountersOnRotateEvent(long hbaseTotalRowsCommitted, long mysqlTotalRowsProccessed) {

        boolean isDelta = replicatorConfiguration.isWriteRecentChangesToDeltaTables();

        final int TESTS_TO_RUN = 2;

        int testsPassed = 0;

        if (isDelta) {

            // TESTS:
            //
            //    (hbaseTotalRowsCommitted - mysqlTotalRowsProcessed) == SUM_PER_DELTA_TABLE(hbaseRowsCommitted)
            //
            //    mysqlTotalRowsProcessed == SUM_PER_MIRRORED_TABLE(hbaseRowsCommitted)

            long sumOfRowsCommitedForDeltaTables    = 0;
            long sumOfRowsCommitedForMirroredTables = 0;

            if (replicatorMetrics.getTotalsPerTable() != null) {
                for (String tableName : replicatorMetrics.getTotalsPerTable().keySet()) {

                    HashMap<Integer, MutableLong> tableTotals = replicatorMetrics.getTotalsPerTable().get(tableName);

                    if (tableName.contains(":")) {
                        LOGGER.info("hbase destination table");
                        if (tableName.contains("delta:")) { // <- convention: all delta tables must go in 'delta' namespace
                            LOGGER.info("Delta table: " + tableName);

                            if (tableTotals != null) {
                                for (Integer metricID : tableTotals.keySet()) {
                                    LOGGER.info("    checking " + Metric.getCounterName(metricID));
                                    if (tableTotals.get(metricID) != null) {
                                        LOGGER.info(
                                                "\n\t\t" + Metric.getCounterName(metricID)
                                                        + " => "
                                                        + tableTotals.get(metricID).getValue()
                                                        + "\n");

                                        if (metricID == Metric.TOTAL_ROW_OPS_SUCCESSFULLY_COMMITED) {
                                            Long deltaCommitedForTable = tableTotals.get(Metric.TOTAL_ROW_OPS_SUCCESSFULLY_COMMITED).getValue();
                                            sumOfRowsCommitedForDeltaTables += deltaCommitedForTable;
                                        }
                                    }
                                }
                            }
                            else {
                                LOGGER.info("No count for table => " + tableName);
                            }
                        }
                        else {
                            LOGGER.info("Mirrored table: " + tableName);

                            if (tableTotals != null) {
                                for (Integer metricID : tableTotals.keySet()) {
                                    LOGGER.info("    checking " + Metric.getCounterName(metricID));
                                    if (tableTotals.get(metricID) != null) {
                                        LOGGER.info(
                                                "\n\t\t" + Metric.getCounterName(metricID)
                                                        + " => "
                                                        + tableTotals.get(metricID).getValue()
                                                        + "\n");

                                        if (metricID == Metric.TOTAL_ROW_OPS_SUCCESSFULLY_COMMITED) {
                                            Long mirroredCommitedForTable = tableTotals.get(Metric.TOTAL_ROW_OPS_SUCCESSFULLY_COMMITED).getValue();
                                            sumOfRowsCommitedForMirroredTables += mirroredCommitedForTable;
                                        }
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

            if ((hbaseTotalRowsCommitted - mysqlTotalRowsProccessed) == sumOfRowsCommitedForDeltaTables) {
                LOGGER.info("PASS");
                testsPassed++;
            }
            else {
                LOGGER.info("FAIL");
            }

            LOGGER.info("Running check point test:" +
                    " \n     mysqlTotalRowsProcessed == SUM_PER_MIRRORED_TABLE(hbaseRowsCommitted)");

            LOGGER.info(mysqlTotalRowsProccessed + " == " + sumOfRowsCommitedForMirroredTables);

            if (mysqlTotalRowsProccessed == sumOfRowsCommitedForMirroredTables) {
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
            if (mysqlTotalRowsProccessed == mysqlTotalRowsProccessed) {
                return true;
            }
            else {
                return false;
            }
        }
    }
}
