package dev.donhk.transform;

import dev.donhk.descriptors.ElasticRowTypeDescriptor;
import dev.donhk.pojos.DagV3;
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

public class JoinEngine {

    private static final Logger LOG = LogManager.getLogger(JoinEngine.class);
    private final DagV3 dagV3;
    private final Map<StreamKey, PCollection<KV<String, ElasticRow>>> dagDefinition;

    public JoinEngine(DagV3 dagV3, Map<StreamKey, PCollection<KV<String, ElasticRow>>> dagDefinition) {
        this.dagV3 = dagV3;
        this.dagDefinition = dagDefinition;
    }

    public static JoinEngine wrapper(DagV3 dagV3,
                                     Map<StreamKey, PCollection<KV<String, ElasticRow>>> dagDefinition) {
        return new JoinEngine(dagV3, dagDefinition);
    }

    public void execute() {
        int i = 0;
        for (String join : dagV3.getJoins()) {
            // user_transactions[stream#.id] outer join car_info[stream#.car_id]
            final Pattern globalPattern = Pattern.compile("\\s*(.*)]\\s+(outer join|inner join)\\s+(.*)]");
            final Matcher globalMatcher = globalPattern.matcher(join);
            if (!globalMatcher.find()) {
                throw new IllegalArgumentException("Invalid join clause: " + join);
            }
            final String[] leftPart = globalMatcher.group(1).split("\\[");
            final String joinType = globalMatcher.group(2);
            final String[] rightType = globalMatcher.group(3).split("\\[");
            LOG.info("Join#{} Left: {}, Join Type: {}, Right: {}", i++, leftPart, joinType, rightType);
            final KV<StreamKey, PCollection<KV<String, ElasticRow>>> collection1 = getKvpCollection(leftPart);
            final KV<StreamKey, PCollection<KV<String, ElasticRow>>> collection2 = getKvpCollection(rightType);

            final PCollection<KV<String, KV<ElasticRow, ElasticRow>>> output;
            if (joinType.contains("outer")) {
                output = Join.fullOuterJoin(
                        joinType,
                        collection1.getValue(),
                        collection2.getValue(),
                        ElasticRow.create(),
                        ElasticRow.create()
                );
            } else {
                output = Join.innerJoin(joinType, collection1.getValue(), collection2.getValue());
            }
            final PCollection<KV<String, ElasticRow>> finalOutput =
                    output.apply("flatten-outer",
                            MapElements.into(TypeDescriptors.kvs(TypeDescriptors.strings(), ElasticRowTypeDescriptor.of()))
                                    .via(RecordFlattener.of()));
            finalOutput.apply(PrintPCollection.with("partial-join"));
            dagDefinition.put(collection1.getKey(), finalOutput);
            dagDefinition.put(collection2.getKey(), finalOutput);
        }
        Map.Entry<StreamKey, PCollection<KV<String, ElasticRow>>> map = dagDefinition.entrySet().iterator().next();
        dagDefinition.clear();
        dagDefinition.put(map.getKey(), map.getValue());

    }

    private KV<StreamKey, PCollection<KV<String, ElasticRow>>> getKvpCollection(String[] parts) {
        final String joinParts = String.join("|", parts);
        LOG.debug("PARTS {}", String.join(",", parts));
        for (Map.Entry<StreamKey, PCollection<KV<String, ElasticRow>>> entry : dagDefinition.entrySet()) {
            final StreamKey streamKey = entry.getKey();
            final String type = parts[0];
            final String streamAndKeyCol = parts[1];
            final String stream = streamAndKeyCol.substring(0, streamAndKeyCol.indexOf("."));
            final boolean typeMatch = streamKey.getType().equalsIgnoreCase(type);
            final boolean streamMatch = streamKey.getName().equalsIgnoreCase(stream);
            LOG.debug("type {} stream {} | {}", type, stream, streamKey.toString());
            if (typeMatch && streamMatch) {
                return KV.of(streamKey, entry.getValue());
            }
        }
        throw new IllegalStateException("No stream found as " + joinParts + " within dag definition, typo?");
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
