package dev.donhk.transform;

import dev.donhk.pojos.ElasticRow;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

import java.util.Map;

public class JoinWrapper {

    private final String join;
    private final Map<String, PCollection<KV<Long, ElasticRow>>> dagDefinition;

    public JoinWrapper(String join, Map<String, PCollection<KV<Long, ElasticRow>>> dagDefinition) {
        this.join = join;
        this.dagDefinition = dagDefinition;
    }

    public static JoinWrapper wrapper(String join, Map<String, PCollection<KV<Long, ElasticRow>>> dagDefinition) {
        return new JoinWrapper(join, dagDefinition);
    }

    public void execute() {

    }
}
