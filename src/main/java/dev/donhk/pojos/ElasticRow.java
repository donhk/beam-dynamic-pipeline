package dev.donhk.pojos;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public class ElasticRow implements Serializable, Cloneable {

    private Map<String, Object> storage;

    public static ElasticRow of() {
        return new ElasticRow();
    }

    private ElasticRow() {
        storage = new TreeMap<>();
    }

    public void addCol(ElasticRowCol key, Object obj) {
        storage.put(key.name(), obj);
    }

    public void addCol(String key, Object obj) {
        storage.put(key, obj);
    }

    public void rmCol(String key) {
        storage.remove(key);
    }

    public boolean hasColumn(String key) {
        return storage.containsKey(key);
    }

    public String getDimension(String key) {
        if (!hasColumn(key)) {
            throw new IllegalArgumentException(key);
        }
        return (String) storage.get(key);
    }

    public double getScalarCol(String key) {
        if (!storage.containsKey(key)) {
            throw new IllegalArgumentException("no " + key + " in " + storage);
        }
        return (double) storage.get(key);
    }

    public Set<String> columns() {
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

    @Override
    public ElasticRow clone() {
        try {
            ElasticRow clone = (ElasticRow) super.clone();
            Map<String, Object> map = new LinkedHashMap<>(clone.storage);
            clone.setStorage(map);
            return clone;
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException(e);
        }
    }

    private void setStorage(Map<String, Object> storage) {
        this.storage = storage;
    }

}
