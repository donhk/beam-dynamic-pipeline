package dev.donhk.core;

import dev.donhk.transform.*;
import dev.donhk.utilities.ConvertToDict;
import dev.donhk.utilities.Dag;
import dev.donhk.utilities.WordRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.concurrent.Callable;

public class AbstractPipeline implements Callable<String> {

    private static final Logger LOG = LogManager.getLogger(AbstractPipeline.class);
    private final Dag dag;
    private final List<String> lines;
    private final List<String> dicLines;

    public static AbstractPipeline of(List<String> lines, List<String> dicLines, Dag dag) {
        return new AbstractPipeline(lines, dicLines, dag);
    }

    private AbstractPipeline(List<String> lines, List<String> dicLines, Dag dag) {
        this.dag = dag;
        this.lines = lines;
        this.dicLines = dicLines;
    }

    @Override
    public String call() {
        // read file -> tokenize -> filter -> count -> get top
        final Pipeline pipeline = Pipeline.create();
        LOG.info("assembling pipeline " + dag.getName());

        final PCollection<String> mainStream =
                pipeline.apply("read file", Create.of(lines));

        final PCollection<KV<String, String>> secondStream =
                pipeline.apply("read dict", Create.of(dicLines))
                        .apply("convert", ConvertToDict.of());

        final PCollection<String> tokens =
                mainStream.apply("convert-lines-to-words", Tokenize.of())
                        .apply("transform-strings", TransformString.upper());

        final PCollection<String> transformed;
        if (dag.getTransform().equalsIgnoreCase("upper")) {
            transformed = tokens.apply("transform-strings-upper", TransformString.upper());
        } else {
            transformed = tokens.apply("transform-strings-lower", TransformString.lower());
        }

        final PCollection<String> filtered =
                transformed.apply("filter strings", FilterWords.with(dag.getFilter()));

        final PCollection<KV<String, Long>> top =
                filtered.apply("count words", Count.perElement())
                        .apply("get top x", TopKElements.of(dag.getTop()));

        final PCollection<WordRecord> words;
        LOG.info(dag.getJoin().name());
        switch (dag.getJoin()) {
            case inner:
                words = top.apply("join", JoinSets.innerJoin(secondStream));
                break;
            case outer:
                words = top.apply("join", JoinSets.outerJoin(secondStream));
                break;
            case none:
            default:
                words = top.apply("no-join", JoinSets.none(secondStream));
                break;
        }

        words.apply("print", PrintPCollection.with(dag.getName()));
        LOG.info("starting pipeline " + dag.getName());

        pipeline.run().waitUntilFinish();
        return dag.getName();
    }
}
