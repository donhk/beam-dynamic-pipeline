package dev.donhk.transform;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class PrintPCollection<T> extends PTransform<PCollection<T>, PDone> {

    private final String prefix;
    private static final Logger LOG = LogManager.getLogger(PrintPCollection.class);

    public static <T> PrintPCollection<T> with() {
        return new PrintPCollection<>(null);
    }

    public static <T> PrintPCollection<T> with(String prefix) {
        return new PrintPCollection<>(prefix);
    }

    private PrintPCollection(String prefix) {
        this.prefix = prefix;
    }

    @Override
    public PDone expand(PCollection<T> input) {
        input.apply(ParDo.of(new PrintElement<>()));
        return PDone.in(input.getPipeline());
    }

    private class PrintElement<A> extends DoFn<A, Void> {
        @ProcessElement
        public void process(@Element A elem) {
            if (prefix == null) {
                LOG.info(elem);
            } else {
                LOG.info("[{}] {}", prefix, elem);
            }
        }
    }
}
