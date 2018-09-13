package com.booking.replication.commons.map;

import java.util.HashMap;
import java.util.Map;

public class MapFlatter {
    private final String delimiter;

    public MapFlatter(String delimiter) {
        this.delimiter = delimiter;
    }

    public Map<String, Object> flattenMap(Map<String, Object> map) {
        Map<String, Object> flattenMap = new HashMap<>();

        this.flattenMap(null, map, flattenMap);

        return flattenMap;
    }

    @SuppressWarnings("unchecked")
    private void flattenMap(String path, Map<String, Object> map, Map<String, Object> flattenMap) {
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            String flattenPath = (path != null) ? String.format("%s%s%s", path, this.delimiter, entry.getKey()) : entry.getKey();

            if (Map.class.isInstance(entry.getValue())) {
                this.flattenMap(flattenPath, Map.class.cast(entry.getValue()), flattenMap);
            } else {
                flattenMap.put(flattenPath, entry.getValue());
            }
        }
    }
}
