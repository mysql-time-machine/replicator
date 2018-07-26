package com.booking.replication.augmenter.model;

import java.io.Serializable;
import java.util.Collection;

@SuppressWarnings("unused")
public class AugmentedEventSchema implements Serializable {
    private Collection<AugmentedEventColumn> columns;
    private String create;

    public AugmentedEventSchema() {
    }

    public AugmentedEventSchema(Collection<AugmentedEventColumn> columns, String create) {
        this.columns = columns;
        this.create = create;
    }

    public Collection<AugmentedEventColumn> getColumns() {
        return this.columns;
    }

    public String getCreate() {
        return this.create;
    }
}
