package dev.donhk.transform;

import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PCollection;

import java.util.regex.Pattern;

public class FilterWords extends PTransform<PCollection<String>, PCollection<String>> {

    private final Pattern expression;

    private FilterWords(String expression) {
        this.expression = Pattern.compile(expression);
    }

    public static FilterWords with(String expression) {
        return new FilterWords(expression);
    }

    @Override
    public PCollection<String> expand(PCollection<String> input) {
        return input.apply(Filter.by((SerializableFunction<String, Boolean>) input1 ->
                expression.matcher(input1).matches()
        ));
    }
}
