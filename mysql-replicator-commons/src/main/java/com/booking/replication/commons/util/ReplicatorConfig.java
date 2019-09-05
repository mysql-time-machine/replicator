package com.booking.replication.commons.util;

import java.util.HashMap;
import java.util.Objects;

public class ReplicatorConfig<K, V> extends HashMap<K, V> {

    public V get(Object key, boolean required) {
        V value = super.get(key);
        if (required) {
            Objects.requireNonNull(value);
        }
        return value;
    }
}
