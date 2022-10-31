package dev.donhk.transform;

import dev.donhk.utilities.WordRecord;
import dev.donhk.utilities.WordRecordTypeDescriptor;
import org.apache.beam.sdk.extensions.joinlibrary.Join;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

//https://blog.knoldus.com/apache-beam-ways-to-join-pcollections/#b-left-outer-join
@SuppressWarnings("unused")
public class JoinSets extends PTransform<PCollection<KV<String, Long>>, PCollection<WordRecord>> {


    private final PCollection<KV<String, String>> dictionary;
    private final JoinType type;

    private JoinSets(PCollection<KV<String, String>> dictionary, JoinType type) {
        this.dictionary = dictionary;
        this.type = type;
    }

    public static JoinSets innerJoin(PCollection<KV<String, String>> dictionary) {
        return new JoinSets(dictionary, JoinType.inner);
    }

    public static JoinSets outerJoin(PCollection<KV<String, String>> dictionary) {
        return new JoinSets(dictionary, JoinType.outer);
    }

    public static JoinSets none(PCollection<KV<String, String>> dictionary) {
        return new JoinSets(dictionary, JoinType.none);
    }

    @Override
    public PCollection<WordRecord> expand(PCollection<KV<String, Long>> mainInput) {
        if (type == JoinType.none) {
            return mainInput.apply(
                    MapElements.into(WordRecordTypeDescriptor.of())
                            .via((SerializableFunction<KV<String, Long>, WordRecord>) (data) -> {
                                WordRecord wordRecord = new WordRecord();
                                wordRecord.addCol("pk", data.getKey());
                                wordRecord.addCol("hits", data.getValue());
                                return wordRecord;
                            }));
        }
        if (type == JoinType.outer) {
            return Join.fullOuterJoin(mainInput, dictionary, -1L, "null")
                    .apply("joining data " + type.name(),
                            MapElements.into(WordRecordTypeDescriptor.of()).via(BuildRecord.of()));
        }
        return Join.innerJoin(mainInput, dictionary)
                .apply("joining data " + type.name(),
                        MapElements.into(WordRecordTypeDescriptor.of()).via(BuildRecord.of()));
    }

    private static class BuildRecord implements SerializableFunction<KV<String, KV<Long, String>>, WordRecord> {

        public static BuildRecord of() {
            return new BuildRecord();
        }

        @Override
        public WordRecord apply(KV<String, KV<Long, String>> joinedSet) {
            WordRecord wordRecord = new WordRecord();
            wordRecord.addCol("pk", joinedSet.getKey());
            wordRecord.addCol("hits", joinedSet.getValue().getKey());
            wordRecord.addCol("att", joinedSet.getValue().getValue());
            return wordRecord;
        }
    }
}




















