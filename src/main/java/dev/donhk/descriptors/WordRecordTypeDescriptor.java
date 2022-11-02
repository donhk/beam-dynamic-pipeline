package dev.donhk.descriptors;

import dev.donhk.pojos.WordRecord;
import org.apache.beam.sdk.values.TypeDescriptor;

public class WordRecordTypeDescriptor extends TypeDescriptor<WordRecord> {
    public static WordRecordTypeDescriptor of() {
        return new WordRecordTypeDescriptor();
    }
}
