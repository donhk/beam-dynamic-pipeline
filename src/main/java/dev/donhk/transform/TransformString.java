package dev.donhk.transform;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

public class TransformString extends PTransform<PCollection<String>, PCollection<String>> {

    private final int type;

    public static TransformString upper() {
        return new TransformString(0);
    }

    public static TransformString lower() {
        return new TransformString(1);
    }

    private TransformString(int type) {
        this.type = type;
    }

    @Override
    public PCollection<String> expand(PCollection<String> input) {
        if (type == 0) {
            return input.apply(ParDo.of(new Upper()));
        }
        return input.apply(ParDo.of(new Lower()));
    }

    private static class Upper extends DoFn<String, String> {
        @ProcessElement
        public void process(@Element String elem, OutputReceiver<String> out) {
            out.output(elem.toUpperCase());
        }
    }

    private static class Lower extends DoFn<String, String> {
        @ProcessElement
        public void process(@Element String elem, OutputReceiver<String> out) {
            out.output(elem.toLowerCase());
        }
    }
}
