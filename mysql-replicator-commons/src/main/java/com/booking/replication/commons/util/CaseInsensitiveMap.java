package com.booking.replication.commons.util;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class CaseInsensitiveMap<V> implements Map<String, V> {

    private final Map<String, V> innerMap;

    public CaseInsensitiveMap() {
        this(new HashMap<>());
    }

    public CaseInsensitiveMap(Map<String, V> innerMap) {
        this.innerMap = innerMap;
    }

    @Override
    public int size() {
        return innerMap.size();
    }

    @Override
    public boolean isEmpty() {
        return innerMap.isEmpty();
    }

    @Override
    public boolean containsKey(Object key) {
        return innerMap.containsKey(verifyAndGetLowercaseKey(key));
    }

    @Override
    public boolean containsValue(Object value) {
        return innerMap.containsValue(value);
    }

    @Override
    public V get(Object key) {
        return innerMap.get(verifyAndGetLowercaseKey(key));
    }

    @Override
    public V put(String key, V value) {
        return innerMap.put(verifyAndGetLowercaseKey(key), value);
    }

    @Override
    public V remove(Object key) {
        return innerMap.remove(verifyAndGetLowercaseKey(key));
    }

    @Override
    public void putAll(Map<? extends String, ? extends V> m) {
        m.forEach((k,v) -> innerMap.put(verifyAndGetLowercaseKey(k), v));
    }

    @Override
    public void clear() {
        innerMap.clear();
    }

    @Override
    public Set<String> keySet() {
        return innerMap.keySet();
    }

    @Override
    public Collection<V> values() {
        return innerMap.values();
    }

    @Override
    public Set<Entry<String, V>> entrySet() {
        return innerMap.entrySet();
    }

    private static String verifyAndGetLowercaseKey(Object key) {
        if (key == null) {
            throw new NullPointerException("Key can't be null");
        }
        if (key instanceof String) {
            return ((String) key).toLowerCase();
        }
        throw new ClassCastException("Expected String, got " + key.getClass());
    }
}
