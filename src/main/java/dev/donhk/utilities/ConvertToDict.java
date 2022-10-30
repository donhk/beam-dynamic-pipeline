package dev.donhk.utilities;

import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;

public class ConvertToDict extends PTransform<PCollection<String>, PCollection<KV<String, String>>> {
    public static ConvertToDict of() {
        return new ConvertToDict();
    }

    private ConvertToDict() {
    }

    @Override
    public PCollection<KV<String, String>> expand(PCollection<String> input) {
        return input.apply(
                MapElements.into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.strings()))
                        .via((SerializableFunction<String, KV<String, String>>) (line) -> {
                            final String[] parts = line.toUpperCase().split(",");
                            final String key = parts[0].trim();
                            final String value = parts[1].trim();
                            return KV.of(key, value);
                        })
        );
    }
}
