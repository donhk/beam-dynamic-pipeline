package dev.donhk.transform;

import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.Top;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

import java.io.Serializable;
import java.util.Comparator;

public class TopKElements extends PTransform<PCollection<KV<String, Long>>, PCollection<KV<String, Long>>> {

    private final int k;

    public static TopKElements of(int k) {
        return new TopKElements(k);
    }

    private TopKElements(int k) {
        this.k = k;
    }

    @Override
    public PCollection<KV<String, Long>> expand(PCollection<KV<String, Long>> input) {
        return input.apply(
                Top.of(k, (Comparator<KV<String, Long>> & Serializable) (record1, record2) ->
                        Long.compare(record1.getValue(), record2.getValue())
                ).withoutDefaults()).apply(Flatten.iterables());
    }
}













