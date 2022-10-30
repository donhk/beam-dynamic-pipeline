package dev.donhk.utilities;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/*
 * key -> val1, val2
 *
 * col1, col2, col3
 * key   val1  val2
 *
 * col1=key
 * col2=val2
 * col3=val3
 */
public class WordRecord implements Serializable {

    private final Map<String, Object> storage;

    public WordRecord() {
        storage = new LinkedHashMap<>();
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
        if (!(o instanceof WordRecord)) {
            return false;
        }
        WordRecord another = (WordRecord) o;
        if (another.storage.size() != storage.size()) {
            return false;
        }

        return storage.entrySet().stream()
                .allMatch(e -> e.getValue().equals(another.storage.get(e.getKey())));
    }
}
