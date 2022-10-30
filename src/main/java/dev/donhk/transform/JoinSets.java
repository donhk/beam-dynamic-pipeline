package dev.donhk.transform;

import dev.donhk.utilities.WordRecord;
import dev.donhk.utilities.WordRecordTypeDescriptor;
import org.apache.beam.sdk.extensions.joinlibrary.Join;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;

//https://blog.knoldus.com/apache-beam-ways-to-join-pcollections/#b-left-outer-join
public class JoinSets extends PTransform<PCollection<KV<String, Long>>, PCollection<WordRecord>> {

    private final PCollection<KV<String, String>> dictionary;

    private JoinSets(PCollection<KV<String, String>> dictionary) {
        this.dictionary = dictionary;
    }

    public static JoinSets of(PCollection<KV<String, String>> dictionary) {
        return new JoinSets(dictionary);
    }

    @Override
    public PCollection<WordRecord> expand(PCollection<KV<String, Long>> mainInput) {
        return Join.innerJoin(mainInput, dictionary)
                .apply("joining data",
                        MapElements
                                .into(WordRecordTypeDescriptor.of())
                                .via((SerializableFunction<KV<String, KV<Long, String>>, WordRecord>) (joinedSet) -> {
                                    WordRecord wordRecord = new WordRecord();
                                    wordRecord.addCol("pk", joinedSet.getKey());
                                    wordRecord.addCol("hits", joinedSet.getValue().getKey());
                                    wordRecord.addCol("att", joinedSet.getValue().getValue());
                                    return wordRecord;
                                }));
    }
}




















