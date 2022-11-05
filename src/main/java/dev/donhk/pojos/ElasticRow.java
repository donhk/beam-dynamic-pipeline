package dev.donhk.pojos;

import java.io.Serializable;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public class ElasticRow implements Serializable {

    private final Map<String, Object> storage;

    public static ElasticRow of() {
        return new ElasticRow();
    }

    private ElasticRow() {
        storage = new TreeMap<>();
    }

    public void addCol(String key, Object obj) {
        storage.put(key, obj);
    }

    public Set<String> keys() {
        return storage.keySet();
    }

    @Override
    public String toString() {
        return storage.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof ElasticRow)) {
            return false;
        }
        ElasticRow another = (ElasticRow) o;
        if (another.storage.size() != storage.size()) {
            return false;
        }

        return storage.entrySet().stream()
                .allMatch(e -> e.getValue().equals(another.storage.get(e.getKey())));
    }
}
