package dev.donhk.transform;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class PrintPCollection<T> extends PTransform<PCollection<T>, PDone> {

    private static final Logger LOG = LogManager.getLogger(PrintPCollection.class);

    public static <T> PrintPCollection<T> with() {
        return new PrintPCollection<>();
    }

    private PrintPCollection() {
    }

    @Override
    public PDone expand(PCollection<T> input) {
        input.apply(ParDo.of(new PrintElement<>()));
        return PDone.in(input.getPipeline());
    }

    private static class PrintElement<T> extends DoFn<T, Void> {
        @ProcessElement
        public void process(@Element T elem) {
            LOG.info(elem);
        }
    }
}
