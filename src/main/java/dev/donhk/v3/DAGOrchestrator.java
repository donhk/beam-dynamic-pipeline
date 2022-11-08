package dev.donhk.v3;

import dev.donhk.elastict.RemoveCol;
import dev.donhk.elastict.SumColumnsKeep;
import dev.donhk.pojos.CarInformation;
import dev.donhk.pojos.Dag;
import dev.donhk.pojos.ElasticRow;
import dev.donhk.pojos.UserTxn;
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

@SuppressWarnings("Duplicates")
public class DAGOrchestrator {
    private static final Logger LOG = LogManager.getLogger(DAGOrchestrator.class);
    private static final int _windowSize = 10;
    private static final int _elements = 20;

    public void execute() {
        // create the unbounded PCollection from TestStream
        final Pipeline pipeline = Pipeline.create();
        // split into windows
        final PCollection<KV<Long, UserTxn>> windowedUserTxn =
                StreamUtils.userTxnWindowData(pipeline, _elements, _windowSize);
        final PCollection<KV<Long, CarInformation>> carInfoWindowData =
                StreamUtils.carInfoWindowData(pipeline, _elements, _windowSize);
        // convert to elastic record
        PCollection<KV<Long, ElasticRow>> elastic = windowedUserTxn.apply(UserTxn2ElasticRow.of());
        PCollection<KV<Long, ElasticRow>> elastic2 = carInfoWindowData.apply(CarInfo2ElasticRow.of());

        elastic2.apply(PrintPCollection.with());

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
