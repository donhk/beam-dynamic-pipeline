package dev.donhk.utilities;

import dev.donhk.transform.JoinType;

import java.io.Serializable;

public class Dag implements Serializable {
    private String transform;
    private String filter;
    private int top;
    private JoinType join;

    public Dag() {
    }

    public String getTransform() {
        return transform;
    }

    public void setTransform(String transform) {
        this.transform = transform;
    }

    public String getFilter() {
        return filter;
    }

    public void setFilter(String filter) {
        this.filter = filter;
    }

    public int getTop() {
        return top;
    }

    public void setTop(int top) {
        this.top = top;
    }

    public JoinType getJoin() {
        return join;
    }

    public void setJoin(JoinType join) {
        this.join = join;
    }

    @Override
    public String toString() {
        return "Dag{" +
                "transform='" + transform + '\'' +
                ", filter='" + filter + '\'' +
                ", top=" + top +
                ", join=" + join +
                '}';
    }
}
