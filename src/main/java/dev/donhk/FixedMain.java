package dev.donhk;

import dev.donhk.core.PipelineBuilder;

public class FixedMain {

    public static void main(String[] args) {
        PipelineBuilder builder = new PipelineBuilder(
                "assets/shakespeare.txt",
                "assets/dictionary.txt"
        );
        builder.execute();
    }
}
