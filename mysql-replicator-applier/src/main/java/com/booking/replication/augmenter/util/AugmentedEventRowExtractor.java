package com.booking.replication.augmenter.util;

import com.booking.replication.augmenter.model.event.AugmentedEvent;
import com.booking.replication.augmenter.model.event.DeleteRowsAugmentedEventData;
import com.booking.replication.augmenter.model.event.UpdateRowsAugmentedEventData;
import com.booking.replication.augmenter.model.event.WriteRowsAugmentedEventData;
import com.booking.replication.augmenter.model.row.AugmentedRow;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class AugmentedEventRowExtractor {

    public static List<AugmentedRow> extractAugmentedRows(AugmentedEvent augmentedEvent) {

        Long commitTimestamp = augmentedEvent.getHeader().getEventTransaction().getTimestamp();

        List<AugmentedRow> augmentedRows = new ArrayList<>();

        switch (augmentedEvent.getHeader().getEventType()) {

            case WRITE_ROWS:
                WriteRowsAugmentedEventData writeRowsAugmentedEventData =
                        ((WriteRowsAugmentedEventData) augmentedEvent.getData());

                Collection<AugmentedRow> extractedAugmentedRowsFromInsert =
                        writeRowsAugmentedEventData.getAugmentedRows();

                // This part overrides the:
                //      - commitTimestamp of all rows in transaction to the
                //        transaction commit time.
                //      - microsecondsTimestamp
                overrideRowsCommitTimeAndSetMicroseconds(commitTimestamp, extractedAugmentedRowsFromInsert);

                augmentedRows.addAll(extractedAugmentedRowsFromInsert);

                break;

            case UPDATE_ROWS:
                UpdateRowsAugmentedEventData updateRowsAugmentedEventData =
                        ((UpdateRowsAugmentedEventData) augmentedEvent.getData());

                Collection<AugmentedRow> extractedAugmentedRowsFromUpdate =
                        updateRowsAugmentedEventData.getAugmentedRows();

                overrideRowsCommitTimeAndSetMicroseconds(
                        commitTimestamp, extractedAugmentedRowsFromUpdate);

                augmentedRows.addAll(extractedAugmentedRowsFromUpdate);

                break;

            case DELETE_ROWS:
                DeleteRowsAugmentedEventData deleteRowsAugmentedEventData =
                        ((DeleteRowsAugmentedEventData) augmentedEvent.getData());

                Collection<AugmentedRow> extractedAugmentedRowsFromDelete =
                        deleteRowsAugmentedEventData.getAugmentedRows();

                overrideRowsCommitTimeAndSetMicroseconds(
                        commitTimestamp, extractedAugmentedRowsFromDelete);

                augmentedRows.addAll(extractedAugmentedRowsFromDelete);

                break;

            default:
                break;
        }
        return augmentedRows;
    }

    private static void overrideRowsCommitTimeAndSetMicroseconds(Long commitTimestamp, Collection<AugmentedRow> extractedAugmentedRowsFromInsert) {
        for (AugmentedRow ar : extractedAugmentedRowsFromInsert) {
            ar.setCommitTimestamp(commitTimestamp);

            Long microsOverride = commitTimestamp * 1000 + ar.getFakeMicrosecondCounter();
            ar.setRowMicrosecondTimestamp(microsOverride);
        }
    }
}
