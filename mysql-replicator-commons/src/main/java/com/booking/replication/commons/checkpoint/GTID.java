package com.booking.replication.commons.checkpoint;

import java.io.Serializable;

@SuppressWarnings("unused")
public class GTID implements Serializable, Comparable<GTID>{
    private GTIDType type;
    private String value;
    private byte flags;

    public GTID() {
    }

    public GTID(GTIDType type, String value, byte flags) {
        this.type = type;
        this.value = value;
        this.flags = flags;
    }

    public GTIDType getType() {
        return this.type;
    }

    public String getValue() {
        return this.value;
    }

    public byte getFlags() {
        return this.flags;
    }

    @Override
    public int compareTo(GTID gtid) {
        return this.type.compare(this, gtid);
    }

    @Override
    public boolean equals(Object gtid) {
        if (GTID.class.isInstance(gtid)) {
            return this.compareTo(GTID.class.cast(gtid)) == 0;
        } else {
            return false;
        }
    }

    @Override
    public String toString() {
        return String.format("gtid: %s", value);
    }
}
