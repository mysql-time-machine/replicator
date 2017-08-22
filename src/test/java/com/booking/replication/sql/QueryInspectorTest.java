package com.booking.replication.sql;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class QueryInspectorTest {

    @Test
    public void makeSureWeCanExtractGtidFromAppropriateBinlogEvents() throws Exception {

        QueryInspector.setIsPseudoGTIDPattern("(?<=_pseudo_gtid_hint__asc\\:)(.{8}\\:.{16}\\:.{8})");

        assertTrue(QueryInspector.isPseudoGTID("use `booking_meta`; drop view if exists `booking_meta`.`_pseudo_gtid_hint__asc:57E404AC:000000002A5B1D64:A4608F96`"));

        assertEquals("57E404AC:000000002A5B1D64:A4608F96",
                QueryInspector.extractPseudoGTID("use `booking_meta`; drop view if exists `booking_meta`.`_pseudo_gtid_hint__asc:57E404AC:000000002A5B1D64:A4608F96`"));
    }
}