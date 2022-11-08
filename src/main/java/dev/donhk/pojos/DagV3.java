package dev.donhk.pojos;

import java.io.Serializable;
import java.util.List;

public class DagV3 implements Serializable {
    private String name;
    private List<String> userTransactions;
    private List<String> carInfo;
    private List<String> joins;
    private List<String> postJoinTransforms;
    private List<String> outputs;

    public DagV3() {
    }

    public String getName() {
        return this.name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<String> getUserTransactions() {
        return userTransactions;
    }

    public void setUserTransactions(List<String> userTransactions) {
        this.userTransactions = userTransactions;
    }

    public List<String> getCarInfo() {
        return carInfo;
    }

    public void setCarInfo(List<String> carInfo) {
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
