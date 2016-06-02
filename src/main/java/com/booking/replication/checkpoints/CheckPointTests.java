package com.booking.replication.checkpoints;

import com.booking.replication.metrics.INameValue;
import com.booking.replication.metrics.ReplicatorMetrics;
import com.booking.replication.metrics.RowTotals;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.util.Map;

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

            Map<String, RowTotals> totalsPerTableSnapshot = replicatorMetrics.getTotalsPerTableSnapshot();

            if (replicatorMetrics.getTotalsPerTableSnapshot() != null) {
                for (String tableName : totalsPerTableSnapshot.keySet()) {

                    RowTotals tableTotals = replicatorMetrics.getTotalsPerTableSnapshot().get(tableName);
                    INameValue[] namesAndValues = tableTotals.getAllNamesAndValues();

                    if (tableName.contains(":")) {
                        if (tableName.contains("delta:")) { // <- convention: all delta tables must go in 'delta' namespace
                            LOGGER.info("HBase Delta Table: " + tableName);

                            for (int i = 0; i < namesAndValues.length; i++)
                            {
                                LOGGER.info(String.format("\t [%s.%s] => %s", tableName, namesAndValues[i].getName(),
                                        namesAndValues[i].getValue()));
                            }

                            sumOfRowsCommitedForDeltaTables = sumOfRowsCommitedForDeltaTables.add(tableTotals.getTotalHbaseRowsAffected().getValue());
                        }
                        else {
                            LOGGER.info("HBase Mirrored Table: " + tableName);

                            for (int i = 0; i < namesAndValues.length; i++)
                            {
                                LOGGER.info(String.format("\t [%s.%s] => %s", tableName, namesAndValues[i].getName(),
                                        namesAndValues[i].getValue()));
                            }

                            sumOfRowsCommitedForMirroredTables = sumOfRowsCommitedForMirroredTables.add(tableTotals.getTotalHbaseRowsAffected().getValue());
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


            if (mysqlTotalRowsProccessed.equals(hbaseTotalRowsCommitted)) {
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
