package dev.donhk.elastict;

import dev.donhk.pojos.ElasticRow;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;

public class RemoveCol extends PTransform<PCollection<KV<Long, ElasticRow>>, PCollection<KV<Long, ElasticRow>>> {

    private final String colName;

    public RemoveCol(String colName) {
        this.colName = colName;
    }

    public static RemoveCol of(String colName) {
        return new RemoveCol(colName);
    }

    @Override
    public PCollection<KV<Long, ElasticRow>> expand(PCollection<KV<Long, ElasticRow>> input) {
        return input.apply(
                MapElements.into(TypeDescriptors.kvs(TypeDescriptors.longs(), TypeDescriptor.of(ElasticRow.class)))
                        .via((SerializableFunction<KV<Long, ElasticRow>, KV<Long, ElasticRow>>) inRow -> {
                            ElasticRow eRow = inRow.getValue().clone();
                            eRow.rmCol(colName);
                            return KV.of(inRow.getKey(), eRow);
                        }));
    }
}
