package dev.donhk.core;

import dev.donhk.utilities.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

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

        final List<AbstractPipeline> pipelines = new ArrayList<>();
        for (Dag dag : dags) {
            pipelines.add(AbstractPipeline.of(lines, dicLines, dag));
        }

        final ExecutorService executorService = Executors.newCachedThreadPool();
        final CompletionService<String> service = new ExecutorCompletionService<>(executorService);
        final List<Future<String>> futures = new ArrayList<>();
        for (AbstractPipeline pipe : pipelines) {
            futures.add(service.submit(pipe));
        }
        try {
            for (int i = 0; i < futures.size(); i++) {
                final Future<String> out = service.take();
                LOG.info("{} {}", i, out.get());
            }
            executorService.shutdownNow();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }

    }
}



























