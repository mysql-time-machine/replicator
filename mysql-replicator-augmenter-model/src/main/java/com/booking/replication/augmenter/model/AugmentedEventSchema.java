package com.booking.replication.augmenter.model;

import java.io.Serializable;
import java.util.List;

@SuppressWarnings("unused")
public class AugmentedEventSchema implements Serializable {
    private List<AugmentedEventColumn> columns;
    private String create;

    public AugmentedEventSchema() {
    }

    public AugmentedEventSchema(List<AugmentedEventColumn> columns, String create) {
        this.columns = columns;
        this.create = create;
    }

    public List<AugmentedEventColumn> getColumns() {
        return this.columns;
    }

    public String getCreate() {
        return this.create;
    }
}
