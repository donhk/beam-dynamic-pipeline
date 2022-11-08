package dev.donhk.v3;

import dev.donhk.elastict.RemoveCol;
import dev.donhk.elastict.SumColumnsKeep;
import dev.donhk.pojos.*;
import dev.donhk.stream.CarInfo2ElasticRow;
import dev.donhk.stream.StreamUtils;
import dev.donhk.stream.UserTxn2ElasticRow;
import dev.donhk.transform.FilterByDimension;
import dev.donhk.transform.PrintPCollection;
import dev.donhk.utilities.RemoveColParser;
import dev.donhk.utilities.SumColumnsKeepParser;
import dev.donhk.utilities.Utils;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.values.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.LinkedHashMap;
import java.util.Map;

public class DAGOrchestrator {
    private static final Logger LOG = LogManager.getLogger(DAGOrchestrator.class);
    private static final int _windowSize = 10;
    private static final int _elements = 20;

    public void execute() {
        // create the unbounded PCollection from TestStream
        final Pipeline pipeline = Pipeline.create();
        // split into windows
        // first pass
        //
        final PCollection<KV<Long, UserTxn>> windowedUserTxn =
                StreamUtils.userTxnWindowData(pipeline, _elements, _windowSize);
        final PCollection<KV<Long, CarInformation>> carInfoWindowData =
                StreamUtils.carInfoWindowData(pipeline, _elements, _windowSize);
        //
        // second pass
        //
        final DagV3 dagV3 = Utils.getDagV3();
        final Map<String, PCollection<KV<Long, ElasticRow>>> dagDefinition = new LinkedHashMap<>();
        //start with userTxn
        userTxnDataSource(dagDefinition, windowedUserTxn, dagV3);
        //next with carInfo
        carInfoDataSource(dagDefinition, carInfoWindowData, dagV3);
        //next joins
        joins(dagDefinition, dagV3);
        //next post joins
        postJoins(dagDefinition, dagV3);
        //next outputs
        outputs(dagDefinition, dagV3);

        LOG.info("Starting pipeline");
        pipeline.run().waitUntilFinish();
    }

    private void joins(Map<String, PCollection<KV<Long, ElasticRow>>> dagDefinition, DagV3 dagV3) {
        LOG.info("joins");
        for (String dag : dagDefinition.keySet()) {
            //PCollection<KV<Long, ElasticRow>> elastic = dagDefinition.get(dags);
            for (String join : dagV3.getJoins()) {
                LOG.info("dag {} join {}", dag, join);
            }
        }
    }

    private void postJoins(Map<String, PCollection<KV<Long, ElasticRow>>> dagDefinition, DagV3 dagV3) {
        LOG.info("post join transforms");
        for (String dag : dagDefinition.keySet()) {
            PCollection<KV<Long, ElasticRow>> elastic = dagDefinition.get(dag);
            for (String transform : dagV3.getPostJoinTransforms()) {
                LOG.info(transform);
                elastic = transforms(elastic, transform);
            }
        }
    }

    private void outputs(Map<String, PCollection<KV<Long, ElasticRow>>> dagDefinition, DagV3 dagV3) {
        LOG.info("outputs");
        for (String dag : dagDefinition.keySet()) {
            PCollection<KV<Long, ElasticRow>> elastic = dagDefinition.get(dag);
            for (String transform : dagV3.getOutputs()) {
                LOG.info(transform);
            }
            elastic.apply(dag, PrintPCollection.with(dag));
        }
    }

    private void userTxnDataSource(Map<String, PCollection<KV<Long, ElasticRow>>> dagDefinition,
                                   PCollection<KV<Long, UserTxn>> windowedUserTxn,
                                   DagV3 dagV3) {
        final String userTxn = "userTxn";
        if (!dagV3.getUserTransactions().isEmpty()) {
            PCollection<KV<Long, ElasticRow>> elastic = windowedUserTxn.apply(UserTxn2ElasticRow.of());
            dagDefinition.put(userTxn, elastic);
            for (String transformation : dagV3.getUserTransactions()) {
                elastic = transforms(elastic, transformation);
            }
        }
    }

    private void carInfoDataSource(Map<String, PCollection<KV<Long, ElasticRow>>> dagDefinition,
                                   PCollection<KV<Long, CarInformation>> carInfoWindowData,
                                   DagV3 dagV3) {
        final String carInfo = "carInfo";
        if (!dagV3.getCarInfo().isEmpty()) {
            PCollection<KV<Long, ElasticRow>> elastic = carInfoWindowData.apply(CarInfo2ElasticRow.of());
            dagDefinition.put(carInfo, elastic);
            for (String transformation : dagV3.getCarInfo()) {
                elastic = transforms(elastic, transformation);
            }
        }
    }

    private PCollection<KV<Long, ElasticRow>> transforms(PCollection<KV<Long, ElasticRow>> elastic,
                                                         String transformation) {
        LOG.info("Applying transformation {}", transformation);
        if (transformation.contains("SumColumns")) {
            final SumColumnsKeepParser parser = new SumColumnsKeepParser(transformation);
            return elastic.apply(transformation, SumColumnsKeep.as(parser.columnNames(), parser.outputCol()));
        }
        if (transformation.contains("RemoveCol")) {
            final RemoveColParser parser = new RemoveColParser(transformation);
            return elastic.apply(transformation, RemoveCol.of(parser.colName()));
        }
        if (transformation.startsWith("FilterByDimension")) {
            return elastic.apply(transformation, FilterByDimension.with(transformation));
        }
        return elastic;
    }

}