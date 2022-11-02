package dev.donhk.core;

import dev.donhk.transform.*;
import dev.donhk.utilities.ConvertToDict;
import dev.donhk.pojos.Dag;
import dev.donhk.pojos.WordRecord;
import org.apache.beam.repackaged.direct_java.runners.core.construction.renderer.PipelineDotRenderer;
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

        PCollection<String> transformed = mainStream.apply("convert-lines-to-words", Tokenize.of());
        for (String transform : dag.getTransforms()) {
            if (transform.equalsIgnoreCase("upper")) {
                LOG.info("[{}] transform-strings-upper", dag.getName());
                transformed = transformed.apply("transform-strings-upper", TransformString.upper());
                continue;
            }
            if (transform.equalsIgnoreCase("lower")) {
                LOG.info("[{}] transform-strings-lower", dag.getName());
                transformed = transformed.apply("transform-strings-lower", TransformString.lower());
                continue;
            }
            if (transform.contains("rep")) {
                LOG.info("[{}] transform-rep-{}", dag.getName(), transform);
                transformed = transformed.apply("transform-rep-" + transform, ReplaceString.with(transform));
            }
        }

        final PCollection<String> filtered =
                transformed.apply("filter strings", FilterWords.with(dag.getFilter()));

        final PCollection<KV<String, Long>> top =
                filtered.apply(dag.getName() + " count words", Count.perElement())
                        .apply(dag.getName() + " get top " + dag.getTop(), TopKElements.of(dag.getTop()));

        final PCollection<WordRecord> words;
        switch (dag.getJoin()) {
            case inner:
                words = top.apply(dag.getName() + " inner join", JoinSets.innerJoin(secondStream));
                break;
            case outer:
                words = top.apply(dag.getName() + " outer join", JoinSets.outerJoin(secondStream));
                break;
            case none:
            default:
                words = top.apply(dag.getName() + "no-join", JoinSets.none(secondStream));
                break;
        }

        words.apply(PrintPCollection.with(dag.getName()));
        String dotString = PipelineDotRenderer.toDotString(pipeline);
        LOG.debug("{}", dotString);
        LOG.info("starting pipeline {}", dag.getName());

        pipeline.run().waitUntilFinish();
        return dag.getName();
    }
}
