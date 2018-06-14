package com.booking.replication.commons.checkpoint;

import java.io.Serializable;

@SuppressWarnings("unused")
public class GTID  implements Serializable, Comparable<GTID>{
    private GTIDType type;
    private String value;
    private byte flags;
    private int index;

    public GTID() {
    }

    public GTID(GTIDType type, String value, byte flags, int index) {
        this.type = type;
        this.value = value;
        this.flags = flags;
        this.index = index;
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

    public int getIndex() {
        return this.index;
    }

    @Override
    public int compareTo(GTID gtid) {
        if (gtid != null) {
            if (this.value != null && gtid.value != null) {
                if (this.value.equals(gtid.value)) {
                    return Integer.compare(this.index, gtid.index);
                } else {
                    return this.value.compareTo(gtid.value);
                }
            } else if (this.value != null) {
                return Integer.MAX_VALUE;
            } else if (gtid.value != null) {
                return Integer.MIN_VALUE;
            } else {
                return 0;
            }
        } else {
            return Integer.MAX_VALUE;
        }
    }

    @Override
    public boolean equals(Object gtid) {
        if (GTID.class.isInstance(gtid)) {
            return this.compareTo(GTID.class.cast(gtid)) == 0;
        } else {
            return false;
        }
    }
}
