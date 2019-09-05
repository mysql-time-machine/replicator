package com.booking.replication.commons.checkpoint;

import java.io.Serializable;
import java.util.Objects;

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
                return 1;
            } else if (binlog.filename != null) {
                return -1;
            } else {
                return 0;
            }
        } else {
            return 1;
        }
    }

    @Override
    public boolean equals(Object binlog) {
        if (binlog instanceof Binlog) {
            return this.compareTo((Binlog) binlog) == 0;
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(filename, position);
    }

    @Override
    public String toString() {
        return String.format("filename: %s | position: %s", filename, position);
    }
}
