package com.booking.replication.commons.util;

import java.util.HashMap;
import java.util.Objects;

public class ReplicatorConfig<K, V> extends HashMap<K, V> {

    public V get(Object k, boolean required){
        V v = super.get(k);
        if(required)
            Objects.requireNonNull(v);
        return v;
    }
}
