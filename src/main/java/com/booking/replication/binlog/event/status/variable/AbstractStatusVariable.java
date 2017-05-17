package com.booking.replication.binlog.event.status.variable;

import com.google.code.or.common.util.ToStringBuilder;

/**
 * Created by bosko on 6/13/17.
 */
public abstract class AbstractStatusVariable {
    protected final int type;

    /**
     *
     */
    public AbstractStatusVariable(int type) {
        this.type = type;
    }

    /**
     *
     */
    @Override
    public String toString() {
        return new ToStringBuilder(this).toString();
    }

    /**
     *
     */
    public int getType() {
        return type;
    }
}
