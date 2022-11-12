package dev.donhk.elastict;

import dev.donhk.pojos.ElasticRow;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;

import java.util.List;

public class SumColumnsKeep
        extends PTransform<PCollection<KV<String, ElasticRow>>, PCollection<KV<String, ElasticRow>>> {

    private final List<String> columns;
    private final String outputCol;

    private SumColumnsKeep(List<String> columns, String outputCol) {
        this.columns = columns;
        this.outputCol = outputCol;
    }

    public static SumColumnsKeep as(List<String> columns, String outputCol) {
        return new SumColumnsKeep(columns, outputCol);
    }

    @Override
    public PCollection<KV<String, ElasticRow>> expand(PCollection<KV<String, ElasticRow>> input) {
        return input.apply("sumColumnsKeep",
                MapElements.into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptor.of(ElasticRow.class)))
                        .via((SerializableFunction<KV<String, ElasticRow>, KV<String, ElasticRow>>) inRow -> {
                            ElasticRow eRow = inRow.getValue().clone();
                            double newColumnValue = 0d;
                            for (String col : columns) {
                                newColumnValue += eRow.getScalarCol(col);
                            }
                            eRow.addCol(outputCol, newColumnValue);
                            return KV.of(inRow.getKey(), eRow);
                        }));
    }
}
