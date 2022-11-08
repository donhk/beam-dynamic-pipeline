package dev.donhk.transform;

import dev.donhk.pojos.ElasticRow;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class UpperDimension extends PTransform<PCollection<KV<Long, ElasticRow>>, PCollection<KV<Long, ElasticRow>>> {
    private static final Logger LOG = LogManager.getLogger(UpperDimension.class);
    private final String transform;

    private UpperDimension(String transform) {
        this.transform = transform;
    }

    public static UpperDimension with(String transform) {
        return new UpperDimension(transform);
    }

    @Override
    public PCollection<KV<Long, ElasticRow>> expand(PCollection<KV<Long, ElasticRow>> input) {
        //Upper[car_model]
        Pattern pattern = Pattern.compile(".*\\[(.*)].*");
        Matcher matcher = pattern.matcher(transform);
        if (!matcher.find()) {
            LOG.info("noop for " + transform);
            return input;
        }
        final String colName = matcher.group(1).toUpperCase(Locale.ENGLISH);
        return input.apply(transform + "-upper", ParDo.of(new Upper(colName)));
    }

    @SuppressWarnings("unused")
    private static class Upper extends DoFn<KV<Long, ElasticRow>, KV<Long, ElasticRow>> {
        private final String colName;

        private Upper(String colName) {
            this.colName = colName;
        }

        @ProcessElement
        public void process(@Element KV<Long, ElasticRow> elem, OutputReceiver<KV<Long, ElasticRow>> out) {
            ElasticRow row = elem.getValue().clone();
            String upper = row.getDimension(colName).toUpperCase(Locale.ENGLISH);
            row.addCol(colName, upper);
            out.output(KV.of(elem.getKey(), row));
        }
    }


}
