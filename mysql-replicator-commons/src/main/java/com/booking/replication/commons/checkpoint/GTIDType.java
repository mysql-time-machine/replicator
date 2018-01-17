package com.booking.replication.commons.checkpoint;

import java.io.Serializable;
import java.util.Comparator;

@SuppressWarnings("unused")
public enum GTIDType implements Serializable, Comparator<GTID> {
    REAL(0) {
        @Override
        public int compareValue(String value1, String value2) {
            String[] transactions1 = value1.substring(GTIDType.UUID_SIZE + 1).split(GTIDType.TRANSACTION_SEPARATOR);
            String[] transactions2 = value2.substring(GTIDType.UUID_SIZE + 1).split(GTIDType.TRANSACTION_SEPARATOR);

            for (int index = 0; index < transactions1.length && index < transactions2.length; index++) {
                if (!transactions1[index].equals(transactions2[index])) {
                    return Long.compare(
                            Long.parseLong(transactions1[index]),
                            Long.parseLong(transactions2[index])
                    );
                }
            }

            return Integer.compare(transactions1.length, transactions2.length);
        }
    },
    PSEUDO(1) {
        @Override
        public int compareValue(String value1, String value2) {
            return value1.compareTo(value2);
        }
    };

    private static final int UUID_SIZE = 36;
    private static final String TRANSACTION_SEPARATOR = "-";

    private final int code;

    GTIDType(int code) {
        this.code = code;
    }

    public int getCode() {
        return this.code;
    }

    @Override
    public int compare(GTID gtid1, GTID gtid2) {
        if (gtid1 != null && gtid2 != null) {
            if (gtid1.getValue() != null && gtid2.getValue() != null) {
                return this.compareValue(gtid1.getValue(), gtid2.getValue());
            } else if (gtid1.getValue() != null) {
                return Integer.MAX_VALUE;
            } else if (gtid2.getValue() != null) {
                return Integer.MIN_VALUE;
            } else {
                return 0;
            }
        } else {
            return Integer.MAX_VALUE;
        }
    }

    protected abstract int compareValue(String value1, String value2);
}
