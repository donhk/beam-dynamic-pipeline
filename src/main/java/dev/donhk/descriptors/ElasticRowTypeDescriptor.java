package dev.donhk.descriptors;

import dev.donhk.pojos.ElasticRow;
import org.apache.beam.sdk.values.TypeDescriptor;

public class ElasticRowTypeDescriptor extends TypeDescriptor<ElasticRow> {
    public static ElasticRowTypeDescriptor of() {
        return new ElasticRowTypeDescriptor();
    }
}
