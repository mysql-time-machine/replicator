package com.booking.replication.applier.hbase.util;

import com.booking.replication.augmenter.model.event.AugmentedEvent;
import com.booking.replication.augmenter.model.event.WriteRowsAugmentedEventData;
import com.booking.replication.augmenter.model.row.AugmentedRow;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class AugmentedEventRowExtractor {

    public static List<AugmentedRow> extractAugmentedRows(AugmentedEvent augmentedEvent) {

        List<AugmentedRow> augmentedRows = new ArrayList<>();

        switch (augmentedEvent.getHeader().getEventType()) {

            case WRITE_ROWS:

                WriteRowsAugmentedEventData writeRowsAugmentedEventData =
                        ((WriteRowsAugmentedEventData) augmentedEvent.getData());

                Collection<AugmentedRow> extractedAugmentedRows = writeRowsAugmentedEventData.getAugmentedRows();

                augmentedRows.addAll(extractedAugmentedRows);

                break;

            case UPDATE_ROWS:
                // TODO
                break;
            case DELETE_ROWS:
                // TODO
                break;
            default:
                break;
        }
        return augmentedRows;
    }
}
