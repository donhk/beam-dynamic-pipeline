package dev.donhk.v3;

import dev.donhk.elastict.RemoveCol;
import dev.donhk.elastict.SumColumnsKeep;
import dev.donhk.pojos.*;
import dev.donhk.stream.CarInfo2ElasticRow;
import dev.donhk.stream.StreamUtils;
import dev.donhk.stream.UserTxn2ElasticRow;
import dev.donhk.transform.PrintPCollection;
import dev.donhk.utilities.RemoveColParser;
import dev.donhk.utilities.SumColumnsKeepParser;
import dev.donhk.utilities.Utils;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.values.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@SuppressWarnings("Duplicates")
public class DAGOrchestrator {
    private static final Logger LOG = LogManager.getLogger(DAGOrchestrator.class);
    private static final int _windowSize = 10;
    private static final int _elements = 20;

    public void execute() {
        // create the unbounded PCollection from TestStream
        final Pipeline pipeline = Pipeline.create();
        // split into windows
        // first pass
        final PCollection<KV<Long, UserTxn>> windowedUserTxn =
                StreamUtils.userTxnWindowData(pipeline, _elements, _windowSize);
        final PCollection<KV<Long, CarInformation>> carInfoWindowData =
                StreamUtils.carInfoWindowData(pipeline, _elements, _windowSize);
        // second pass
        final Map<String, PCollection<KV<Long, ElasticRow>>> dagDefinition = new LinkedHashMap<>();

        // convert to elastic record
        PCollection<KV<Long, ElasticRow>> elastic = windowedUserTxn.apply(UserTxn2ElasticRow.of());
        PCollection<KV<Long, ElasticRow>> elastic2 = carInfoWindowData.apply(CarInfo2ElasticRow.of());
        DagV3 dagV3 = Utils.getDagV3();

        final Dag dag = Utils.getElasticDag();
        for (String transformation : dag.getTransforms()) {
            LOG.info("Applying transformation {}", transformation);
            if (transformation.contains("SumColumns")) {
                final SumColumnsKeepParser parser = new SumColumnsKeepParser(transformation);
                elastic = elastic.apply(transformation, SumColumnsKeep.as(parser.columnNames(), parser.outputCol()));
            }
            if (transformation.contains("RemoveCol")) {
                final RemoveColParser parser = new RemoveColParser(transformation);
                elastic = elastic.apply(transformation, RemoveCol.of(parser.colName()));
            }
        }

        elastic.apply(PrintPCollection.with());
        LOG.info("Starting pipeline");
        pipeline.run().waitUntilFinish();
    }

}
