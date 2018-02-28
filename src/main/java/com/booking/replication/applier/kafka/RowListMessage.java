package com.booking.replication.applier.kafka;

//import com.booking.replication.applier.KafkaApplier;
import com.booking.replication.augmenter.AugmentedRow;
import com.booking.replication.util.JsonBuilder;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Created by bosko on 8/4/16.
 */
public class RowListMessage {


    private static final Logger LOGGER = LoggerFactory.getLogger(RowListMessage.class);
    // metadata
    private int     messageSize;
    private String  messageBinlogPositionID;

    private String  firstRowBinlogPositionID;
    private String  lastRowBinlogPositionID;

    private boolean isOpen;

    // payload
    private List<AugmentedRow> rows;


    @JsonCreator
    public RowListMessage(
            @JsonProperty("messageSize") int messageSize,
            @JsonProperty("rows") List<AugmentedRow> rowsInitialBucket) {

        // init meta
        this.messageSize              = messageSize;
        this.firstRowBinlogPositionID = rowsInitialBucket.get(0).getRowBinlogPositionID();
        this.messageBinlogPositionID  = "M-" + firstRowBinlogPositionID;
        this.isOpen                   = true; // TODO: add separate 'committed' property

        // init payload
        rows                          = rowsInitialBucket;
    }

    public static RowListMessage fromJSON(String jsonString) {
        return JsonBuilder.rowListMessageFromJSON(jsonString);
    }

    public String toJSON() {
        String json = JsonBuilder.rowListMessageToJSON(this);
        return json;
    }

    @JsonIgnore
    public boolean isFull() {

        LOGGER.debug("isFull check: messageSize " + messageSize + ", rows.size " + rows.size() );

        return (!(messageSize > rows.size()));
    }

    public void closeMessageBuffer() {
        this.isOpen = false;

        // set the last row position metadata
        AugmentedRow lastRow =  this.rows.get(rows.size() - 1);
        this.lastRowBinlogPositionID = lastRow.getRowBinlogPositionID();
    }

    public void addRowToMessage(AugmentedRow row) throws KafkaMessageBufferException {
        if (isOpen == true) {
            rows.add(row);
        } else {
            throw new KafkaMessageBufferException("Can't write to a closed message buffer!");
        }
    }

    public int getMessageSize() {
        return messageSize;
    }

    public String getMessageBinlogPositionID() {
        return messageBinlogPositionID;
    }

    public String getFirstRowBinlogPositionID() {
        return firstRowBinlogPositionID;
    }

    public String getLastRowBinlogPositionID() {
        return lastRowBinlogPositionID;
    }

    public void setLastRowBinlogPositionID(String lastRowBinlogPositionID) {
        this.lastRowBinlogPositionID = lastRowBinlogPositionID;
    }

    public List<AugmentedRow> getRows() {
        return rows;
    }

    public boolean isOpen() {
        return isOpen;
    }

    public void setOpen(boolean open) {
        isOpen = open;
    }

    public void setMessageSize(int messageSize) {
        this.messageSize = messageSize;
    }

    public void setMessageBinlogPositionID(String messageBinlogPositionID) {
        this.messageBinlogPositionID = messageBinlogPositionID;
    }

    public void setFirstRowBinlogPositionID(String firstRowBinlogPositionID) {
        this.firstRowBinlogPositionID = firstRowBinlogPositionID;
    }
}
