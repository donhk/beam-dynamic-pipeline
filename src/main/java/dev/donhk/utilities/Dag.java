package dev.donhk.utilities;

import dev.donhk.transform.JoinType;

import java.io.Serializable;
import java.util.List;

public class Dag implements Serializable {
    private String name;
    private List<String> transforms;
    private String filter;
    private int top;
    private JoinType join;

    public Dag() {
    }

    public List<String> getTransforms() {
        return transforms;
    }

    public void setTransforms(List<String> transforms) {
        this.transforms = transforms;
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
                "transforms='" + transforms + '\'' +
                ", filter='" + filter + '\'' +
                ", top=" + top +
                ", join=" + join +
                '}';
    }

    public String getName() {
        return this.name;
    }

    public void setName(String name) {
        this.name = name;
    }
}
