package com.booking.replication.supplier.mysql.binlog;

import com.booking.replication.commons.checkpoint.Checkpoint;
import com.github.shyiko.mysql.binlog.GtidSet;
import org.junit.Test;
import org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.booking.replication.supplier.mysql.binlog.gtid.GtidSetAlgebra;

import static org.junit.Assert.assertEquals;

public class BinaryLogSupplierTest {

    @Test
    public void testGtidSetAlgebra() throws IOException {

        GtidSetAlgebra gtidSetAlgebra = new GtidSetAlgebra();

        // incoming gtidSets
        // 10
        String gtidSet1 = "1044e433-f884-11e6-ad5e-246e962b85ec:1-10,2044e433-f884-11e6-ad5e-246e962b85ec:1-10";
        // 11, 12
        String gtidSet2 = "1044e433-f884-11e6-ad5e-246e962b85ec:1-10,2044e433-f884-11e6-ad5e-246e962b85ec:1-11";
        String gtidSet3 = "1044e433-f884-11e6-ad5e-246e962b85ec:1-10,2044e433-f884-11e6-ad5e-246e962b85ec:1-12";
        // 13: is missing for server 2044e433-f884-11e6-ad5e-246e962b85ec, so the
        // transaction 13 is not yet committed by the applier
        // 14
        String gtidSet4 = "1044e433-f884-11e6-ad5e-246e962b85ec:1-10,2044e433-f884-11e6-ad5e-246e962b85ec:1-14";

        List<Checkpoint> seenCheckpoints = new ArrayList<>();
        seenCheckpoints.add(new Checkpoint(gtidSet1));
        seenCheckpoints.add(new Checkpoint(gtidSet2));
        seenCheckpoints.add(new Checkpoint(gtidSet3));
        seenCheckpoints.add(new Checkpoint(gtidSet4));

        Checkpoint safeCheckpoint = gtidSetAlgebra.getSafeCheckpoint(seenCheckpoints);

        String safeGTIDSet = safeCheckpoint.getGtidSet();

        assertEquals("1044e433-f884-11e6-ad5e-246e962b85ec:1-10,2044e433-f884-11e6-ad5e-246e962b85ec:1-12", safeGTIDSet);

    }

}
