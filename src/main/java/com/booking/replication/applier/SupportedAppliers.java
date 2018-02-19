package com.booking.replication.applier;

/**
 * Created by bdevetak on 2/19/18.
 */
public class SupportedAppliers {
    public enum ApplierName {
        HBaseApplier,
        KafkaApplier,
        StdoutJsonApplier,
        DummyApplier
    }
}
