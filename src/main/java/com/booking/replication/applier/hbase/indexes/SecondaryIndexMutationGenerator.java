package com.booking.replication.applier.hbase.indexes;

import com.booking.replication.Configuration;
import com.booking.replication.applier.hbase.HBaseApplierMutationGenerator;
import com.booking.replication.applier.hbase.PutMutation;
import com.booking.replication.augmenter.AugmentedRow;

import java.util.List;

/**
 * Created by bosko on 12/29/16.
 */
public interface SecondaryIndexMutationGenerator {

    List<PutMutation> getPutsForSecondaryIndex(
            Configuration configuration,
            AugmentedRow row,
            String indexName
    );
}

