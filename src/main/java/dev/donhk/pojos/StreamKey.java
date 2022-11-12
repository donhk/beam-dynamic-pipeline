package dev.donhk.pojos;

import java.io.Serializable;

public class StreamKey implements Serializable {
    private final String type;
    private final String name;

    public StreamKey(String type, String name) {
        this.type = type;
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public String getType() {
        return type;
    }



    @Override
    public String toString() {
        return "StreamKey{" +
                "type='" + type + '\'' +
                ", name='" + name + '\'' +
                '}';
    }
}
