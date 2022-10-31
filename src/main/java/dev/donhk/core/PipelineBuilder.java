package dev.donhk.core;

import dev.donhk.transform.*;
import dev.donhk.utilities.*;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

/**
 * Get the 10 most/lest common words that matches a pattern
 */
public class PipelineBuilder {

    private static final Logger LOG = LogManager.getLogger(PipelineBuilder.class);
    private final String sourceFile;
    private final String dictionary;

    public PipelineBuilder(String sourceFile, String dictionary) {
        this.sourceFile = sourceFile;
        this.dictionary = dictionary;
    }

    public void execute() {
        LOG.info("start");
        final List<String> lines = Utils.readFile(sourceFile);
        final List<String> dicLines = Utils.readFile(dictionary);
        final List<Dag> dags = Utils.getDags();
        LOG.info(dags);

        // read file -> tokenize -> filter -> count -> get top
        final Pipeline pipeline = Pipeline.create();

        final PCollection<String> inputText = pipeline.apply("read file", Create.of(lines));
        final PCollection<KV<String, String>> dictionary =
                pipeline.apply("read dict", Create.of(dicLines))
                        .apply("convert", ConvertToDict.of());

        //dictionary.apply(PrintPCollection.with());

        PCollection<WordRecord> words =
                inputText.apply("convert-lines-to-words", Tokenize.of())
                        .apply("transform-strings", TransformString.upper())
                        .apply("filter strings", FilterWords.with(".*DEA.*"))
                        .apply("count words", Count.perElement())
                        .apply("get top x", TopKElements.of(7))
                        .apply("join", JoinSets.outerJoin(dictionary));
        words.apply(PrintPCollection.with());
        pipeline.run().waitUntilFinish();

    }
}



























