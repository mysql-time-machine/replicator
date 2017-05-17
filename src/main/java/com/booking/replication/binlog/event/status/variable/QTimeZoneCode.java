package com.booking.replication.binlog.event.status.variable;

import com.github.shyiko.mysql.binlog.io.ByteArrayInputStream;
import com.google.code.or.common.util.MySQLConstants;

import com.google.code.or.common.util.ToStringBuilder;
import com.booking.replication.binlog.common.cell.StringCell;

import java.io.IOException;

/**
 * Created by bosko on 6/13/17.
 */
public class QTimeZoneCode
        extends  com.booking.replication.binlog.event.status.variable.AbstractStatusVariable {

    public static final int TYPE = MySQLConstants.Q_TIME_ZONE_CODE;

    //
    private final StringCell timeZone;

    /**
     *
     */
    public QTimeZoneCode(StringCell timeZone) {
        super(TYPE);
        this.timeZone = timeZone;
    }

    /**
     *
     */
    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("timeZone", timeZone).toString();
    }

    /**
     *
     */
    public StringCell getTimeZone() {
        return timeZone;
    }

    public static QTimeZoneCode valueOf(ByteArrayInputStream inputStream) throws IOException {
        final int length = inputStream.readInteger(1);  // Length

        byte[] timeZoneBytes = inputStream.read(length);
        return new QTimeZoneCode(StringCell.valueOf(timeZoneBytes));
    }

}
