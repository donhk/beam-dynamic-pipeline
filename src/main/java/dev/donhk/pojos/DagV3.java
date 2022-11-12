package dev.donhk.pojos;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class DagV3 implements Serializable {
    private String name;
    private Map<String, List<String>> userTransactions = Collections.emptyMap();
    private Map<String, List<String>> carInfo = Collections.emptyMap();
    private List<String> joins = Collections.emptyList();
    private List<String> postJoinTransforms = Collections.emptyList();
    private List<String> outputs = Collections.emptyList();

    public DagV3() {
    }

    public String getName() {
        return this.name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Map<String, List<String>> getUserTransactions() {
        return userTransactions;
    }

    public void setUserTransactions(Map<String, List<String>> userTransactions) {
        this.userTransactions = userTransactions;
    }

    public Map<String, List<String>> getCarInfo() {
        return carInfo;
    }

    public void setCarInfo(Map<String, List<String>> carInfo) {
        this.carInfo = carInfo;
    }

    public List<String> getJoins() {
        return joins;
    }

    public void setJoins(List<String> joins) {
        this.joins = joins;
    }

    public List<String> getOutputs() {
        return outputs;
    }

    public void setOutputs(List<String> outputs) {
        this.outputs = outputs;
    }

    public List<String> getPostJoinTransforms() {
        return postJoinTransforms;
    }

    public void setPostJoinTransforms(List<String> postJoinTransforms) {
        this.postJoinTransforms = postJoinTransforms;
    }

    @Override
    public String toString() {
        return "DagV3{" +
                "name='" + name + '\'' +
                ", userTransactions=" + userTransactions +
                ", carInfo=" + carInfo +
                ", joins=" + joins +
                ", postJoinTransforms=" + postJoinTransforms +
                ", outputs=" + outputs +
                '}';
    }
}
