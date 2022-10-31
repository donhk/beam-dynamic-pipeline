package dev.donhk.transform;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ReplaceString extends PTransform<PCollection<String>, PCollection<String>> {

    private static final Logger LOG = LogManager.getLogger(ReplaceString.class);
    private final String first;
    private final String second;

    private ReplaceString(String pattern) {
        final String[] p = pattern.split(",");
        first = p[1];
        second = p[2];
    }

    public static ReplaceString with(String pattern) {
        return new ReplaceString(pattern);
    }

    @Override
    public PCollection<String> expand(PCollection<String> input) {
        return input.apply(ParDo.of(new ReplaceStr()));
    }

    public class ReplaceStr extends DoFn<String, String> {
        @ProcessElement
        public void process(@Element String elem, OutputReceiver<String> out) {
            if (elem.contains(first)) {
                final String newStr = elem.replace(first, second);
                LOG.debug("REP old {} -> new {}", elem, newStr);
                out.output(newStr);
            } else {
                out.output(elem);
            }
        }
    }
}
