package com.booking.replication.augmenter.active.schema;

import com.booking.replication.augmenter.model.AugmentedEventData;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

public class ActiveSchemaTransaction {
    private final Map<String, Queue<AugmentedEventData>> cache;

    public ActiveSchemaTransaction() {
        this.cache = new ConcurrentHashMap<>();
    }

    public boolean begin(String code) {
        return this.cache.put(code, new ConcurrentLinkedQueue<>()) == null;
    }

    public boolean add(String code, AugmentedEventData data) {
        if (this.cache.containsKey(code)) {
            return this.cache.get(code).add(data);
        } else {
            return false;
        }
    }

    public AugmentedEventData[] end(String code) {
        return this.cache.remove(code).toArray(new AugmentedEventData[this.cache.get(code).size()]);
    }
}
