package dev.donhk.transform;

import dev.donhk.pojos.ElasticRow;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class FilterByDimension extends PTransform<PCollection<KV<Long, ElasticRow>>, PCollection<KV<Long, ElasticRow>>> {
    private static final Logger LOG = LogManager.getLogger(FilterByDimension.class);
    private final String transform;

    private FilterByDimension(String transform) {
        this.transform = transform;
    }

    public static FilterByDimension with(String transform) {
        return new FilterByDimension(transform);
    }

    @Override
    public PCollection<KV<Long, ElasticRow>> expand(PCollection<KV<Long, ElasticRow>> input) {
        // FilterByDimension[first_name like '*.o.*']
        final Pattern regexPattern = Pattern.compile(".*'(.*)'.*");
        final Pattern colNamePattern = Pattern.compile(".*\\[(.*)\\s+like.*]");
        final Matcher regexMatcher = regexPattern.matcher(transform);
        final Matcher colNameMatcher = colNamePattern.matcher(transform);
        if (!regexMatcher.find() || !colNameMatcher.find()) {
            return input;
        }
        final String regex = regexMatcher.group(1);
        final String colName = colNameMatcher.group(1).toUpperCase(Locale.ENGLISH);
        return input.apply(transform,
                Filter.by((SerializableFunction<KV<Long, ElasticRow>, Boolean>) wrap -> {
                    final ElasticRow row = wrap.getValue();
                    if (!row.hasColumn(colName)) {
                        return false;
                    }
                    final String string = row.getDimension(colName);
                    final Pattern pattern1 = Pattern.compile(regex);
                    final Matcher matcher1 = pattern1.matcher(string);
                    final boolean find = matcher1.find();
                    LOG.debug("string {}, colName {}, regex {}, matches {}", string, colName, regex, find);
                    return find;
                }));
    }
}
