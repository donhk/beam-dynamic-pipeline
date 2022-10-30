package dev.donhk.transform;

import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;


public class Tokenize extends PTransform<PCollection<String>, PCollection<String>> {
    public static Tokenize of() {
        return new Tokenize();
    }

    @Override
    public PCollection<String> expand(PCollection<String> input) {
        //converts
        return input.apply(FlatMapElements.into(TypeDescriptors.strings())
                .via(new Splitter()));
    }

    private static class Splitter implements SerializableFunction<String, List<String>> {

        @Override
        public List<String> apply(String input) {
            return Arrays.stream(input.split("\\W+"))
                    .filter(Objects::nonNull)
                    .map(String::trim)
                    .filter(s -> s.length() > 0)
                    .collect(Collectors.toList());
        }
    }
}






























