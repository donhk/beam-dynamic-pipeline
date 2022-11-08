package dev.donhk.transform;

import dev.donhk.pojos.ElasticRow;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

public class JoinCollections extends PTransform<PCollection<KV<Long, ElasticRow>>, PCollection<KV<Long, ElasticRow>>> {

    private final String join;

    public JoinCollections(String join) {
        this.join = join;
    }

    public static JoinCollections as(String join) {
        return new JoinCollections(join);
    }

    @Override
    public PCollection<KV<Long, ElasticRow>> expand(PCollection<KV<Long, ElasticRow>> input) {
        return null;
    }
}
