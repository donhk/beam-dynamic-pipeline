package dev.donhk.transform;

import dev.donhk.descriptors.ElasticRowTypeDescriptor;
import dev.donhk.pojos.ElasticRow;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RenameCol extends PTransform<PCollection<KV<String, ElasticRow>>, PCollection<KV<String, ElasticRow>>> {

    private static final Logger LOG = LogManager.getLogger(RenameCol.class);
    private static final Pattern pattern = Pattern.compile("RenameCol\\[\\s*(.*)\\s*,\\s*(.*)\\s*]");
    private final String transform;

    private RenameCol(String transform) {
        this.transform = transform;
    }

    public static RenameCol with(String transform) {
        return new RenameCol(transform);
    }

    @Override
    public PCollection<KV<String, ElasticRow>> expand(PCollection<KV<String, ElasticRow>> input) {
        // RenameCol[id,stream2_id]
        final Matcher matcher = pattern.matcher(transform);
        if (!matcher.find()) {
            return input;
        }
        return input.apply(transform,
                MapElements.into(TypeDescriptors.kvs(TypeDescriptors.strings(), ElasticRowTypeDescriptor.of()))
                        .via((SerializableFunction<KV<String, ElasticRow>, KV<String, ElasticRow>>) record -> {
                            final String oldKey = matcher.group(1);
                            final String newKey = matcher.group(2);
                            final ElasticRow row = record.getValue().clone();
                            row.addCol(newKey, row.getObject(oldKey));
                            row.rmCol(oldKey);
                            return KV.of(record.getKey(), row);
                        }));
    }
}
