package com.booking.replication.commons.map;

import java.util.Map;
import java.util.stream.Collectors;

public class MapFilter {
    private final Map<String, ?> map;

    public MapFilter(Map<String, ?> map) {
        this.map = map;
    }

    public Map<String, Object> filter(String prefix) {
        return this.map.entrySet().stream().filter(
            entry -> entry.getKey().startsWith(prefix)
        ).collect(Collectors.toMap(
            entry -> entry.getKey().substring(prefix.length()), Map.Entry::getValue
        ));
    }
}
