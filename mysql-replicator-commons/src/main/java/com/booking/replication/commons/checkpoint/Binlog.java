package com.booking.replication.commons.checkpoint;

import java.io.Serializable;

public class Binlog implements Serializable, Comparable<Binlog> {
    private String filename;
    private long position;

    public Binlog() {
    }

    public Binlog(String filename, long position) {
        this.filename = filename;
        this.position = position;
    }

    public String getFilename() {
        return this.filename;
    }

    public long getPosition() {
        return this.position;
    }

    @Override
    public int compareTo(Binlog binlog) {
        if (binlog != null) {
            if (this.filename != null && binlog.filename != null) {
                if (this.filename.equals(binlog.filename)) {
                    return Long.compare(this.position, binlog.position);
                } else {
                    return this.filename.compareTo(binlog.filename);
                }
            } else if (this.filename != null) {
                return Integer.MAX_VALUE;
            } else if (binlog.filename != null) {
                return Integer.MIN_VALUE;
            } else {
                return 0;
            }
        } else {
            return Integer.MAX_VALUE;
        }
    }

    @Override
    public boolean equals(Object binlog) {
        if (Binlog.class.isInstance(binlog)) {
            return this.compareTo(Binlog.class.cast(binlog)) == 0;
        } else {
            return false;
        }
    }

    @Override
    public String toString() {
        return String.format("filename: %s | position: %s", filename, position);
    }
}
