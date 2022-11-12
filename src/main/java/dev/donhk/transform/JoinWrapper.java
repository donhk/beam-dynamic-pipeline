package dev.donhk.transform;

import dev.donhk.descriptors.ElasticRowTypeDescriptor;
import dev.donhk.pojos.ElasticRow;
import dev.donhk.pojos.StreamKey;
import org.apache.beam.sdk.extensions.joinlibrary.Join;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class JoinWrapper {

    private static final Logger LOG = LogManager.getLogger(JoinWrapper.class);
    private final String join;
    private final Map<StreamKey, PCollection<KV<String, ElasticRow>>> dagDefinition;

    public JoinWrapper(String join, Map<StreamKey, PCollection<KV<String, ElasticRow>>> dagDefinition) {
        this.join = join;
        this.dagDefinition = dagDefinition;
    }

    public static JoinWrapper wrapper(String join,
                                      Map<StreamKey, PCollection<KV<String, ElasticRow>>> dagDefinition) {
        return new JoinWrapper(join, dagDefinition);
    }

    public void execute() {
        // user_transactions[id] outer join car_info[car_id]
        final Pattern pattern1 = Pattern.compile("\\s*(.*)]\\s+(outer join|inner join)\\s+(.*)]");
        final Matcher matcher1 = pattern1.matcher(join);
        if (!matcher1.find()) {
            throw new IllegalArgumentException("Invalid join clause: " + join);
        }
        final String[] leftPart = matcher1.group(1).split("\\[");
        final String joinType = matcher1.group(2);
        final String[] rightType = matcher1.group(3).split("\\[");
        LOG.info("left: {}, join: {}, right: {}", leftPart, joinType, rightType);

        final PCollection<KV<String, ElasticRow>> collection1 = getKvpCollection(leftPart);
        final PCollection<KV<String, ElasticRow>> collection2 = getKvpCollection(rightType);
        if (collection1 == null || collection2 == null) {
            throw new IllegalArgumentException("collection1 or collection2 is null");
        }

        final PCollection<KV<String, KV<ElasticRow, ElasticRow>>> output;
        if (joinType.contains("outer")) {
            output = Join.fullOuterJoin(
                    joinType,
                    collection1,
                    collection2,
                    ElasticRow.create(),
                    ElasticRow.create()
            );
        } else {
            output = Join.innerJoin(joinType, collection1, collection2);
        }

        PCollection<KV<String, ElasticRow>> finalOutput =
                output.apply("flatten-outer",
                        MapElements.into(TypeDescriptors.kvs(TypeDescriptors.strings(), ElasticRowTypeDescriptor.of()))
                                .via(RecordFlattener.of()));
        final StreamKey key = new StreamKey("joined", "set");
        dagDefinition.clear();
        dagDefinition.put(key, finalOutput);
    }

    private PCollection<KV<String, ElasticRow>> getKvpCollection(String[] parts) {
        for (Map.Entry<StreamKey, PCollection<KV<String, ElasticRow>>> entry : dagDefinition.entrySet()) {
            if (entry.getKey().getType().equalsIgnoreCase(parts[0])) {
                return entry.getValue();
            }
        }
        return null;
    }

    private static class RecordFlattener
            implements SerializableFunction<
            KV<String, KV<ElasticRow, ElasticRow>>,
            KV<String, ElasticRow>
            > {
        public static RecordFlattener of() {
            return new RecordFlattener();
        }

        @Override
        public KV<String, ElasticRow> apply(KV<String, KV<ElasticRow, ElasticRow>> input) {
            final ElasticRow eRow = ElasticRow.create();
            ElasticRow first = input.getValue().getKey();
            eRow.merge(first);
            ElasticRow second = input.getValue().getValue();
            eRow.merge(second);
            return KV.of(input.getKey(), eRow);
        }
    }
}
