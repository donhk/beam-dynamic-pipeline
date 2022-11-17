package dev.donhk.v3;

import dev.donhk.elastict.RemoveCol;
import dev.donhk.elastict.SumColumnsKeep;
import dev.donhk.pojos.*;
import dev.donhk.stream.CarInfo2ElasticRow;
import dev.donhk.stream.StreamUtils;
import dev.donhk.stream.UserTxn2ElasticRow;
import dev.donhk.transform.FilterByDimension;
import dev.donhk.transform.JoinEngine;
import dev.donhk.transform.PrintPCollection;
import dev.donhk.transform.UpperDimension;
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

public class DAGOrchestrator {
    private static final Logger LOG = LogManager.getLogger(DAGOrchestrator.class);
    private static final int _windowSize = 10;
    private static final int _elements = 20;
    private static final String userTxn = "user_transactions";
    private static final String carInfo = "car_info";

    public void execute() {
        // create the unbounded PCollection from TestStream
        final Pipeline pipeline = Pipeline.create();
        // split into windows
        // first pass
        //
        final PCollection<KV<String, UserTxn>> windowedUserTxn =
                StreamUtils.userTxnWindowData(pipeline, _elements, _windowSize);
        final PCollection<KV<String, CarInformation>> carInfoWindowData =
                StreamUtils.carInfoWindowData(pipeline, _elements, _windowSize);
        //
        // second pass
        //
        final DagV3 dagV3 = Utils.getDagV3();
        final Map<StreamKey, PCollection<KV<String, ElasticRow>>> dagDefinition = new LinkedHashMap<>();
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

    private void joins(Map<StreamKey, PCollection<KV<String, ElasticRow>>> dagDefinition, DagV3 dagV3) {
        LOG.info("joins");
        if (dagV3.getJoins().isEmpty()) {
            return;
        }
        JoinEngine.wrapper(dagV3, dagDefinition).execute();
    }

    private void postJoins(Map<StreamKey, PCollection<KV<String, ElasticRow>>> dagDefinition, DagV3 dagV3) {
        LOG.info("post join transforms");
        for (Map.Entry<StreamKey, PCollection<KV<String, ElasticRow>>> entry : dagDefinition.entrySet()) {
            PCollection<KV<String, ElasticRow>> elastic = entry.getValue();
            for (String transform : dagV3.getPostJoinTransforms()) {
                LOG.info(transform);
                elastic = transforms(entry.getKey(), elastic, transform);
                dagDefinition.put(entry.getKey(), elastic);
            }
        }
    }

    private void outputs(Map<StreamKey, PCollection<KV<String, ElasticRow>>> dagDefinition, DagV3 dagV3) {
        LOG.info("outputs");
        for (Map.Entry<StreamKey, PCollection<KV<String, ElasticRow>>> entry : dagDefinition.entrySet()) {
            PCollection<KV<String, ElasticRow>> elastic = entry.getValue();
            for (String transform : dagV3.getOutputs()) {
                LOG.info(transform);
            }
            elastic.apply(entry.getKey().toString(), PrintPCollection.with(entry.getKey().toString()));
            dagDefinition.put(entry.getKey(), elastic);
        }
    }

    private void userTxnDataSource(Map<StreamKey, PCollection<KV<String, ElasticRow>>> dagDefinition,
                                   PCollection<KV<String, UserTxn>> windowedUserTxn,
                                   DagV3 dagV3) {
        if (dagV3.getUserTransactions().isEmpty()) {
            return;
        }
        PCollection<KV<String, ElasticRow>> elastic = windowedUserTxn.apply(UserTxn2ElasticRow.of());
        for (Map.Entry<String, List<String>> streamDag : dagV3.getUserTransactions().entrySet()) {
            final StreamKey key = new StreamKey(userTxn, streamDag.getKey());
            dagDefinition.put(key, elastic);
            for (String transform : streamDag.getValue()) {
                elastic = transforms(key, elastic, transform);
                dagDefinition.put(key, elastic);
            }
        }
    }

    private void carInfoDataSource(Map<StreamKey, PCollection<KV<String, ElasticRow>>> dagDefinition,
                                   PCollection<KV<String, CarInformation>> carInfoWindowData,
                                   DagV3 dagV3) {
        if (dagV3.getCarInfo().isEmpty()) {
            return;
        }
        PCollection<KV<String, ElasticRow>> elastic = carInfoWindowData.apply(CarInfo2ElasticRow.of());
        for (Map.Entry<String, List<String>> streamDag : dagV3.getCarInfo().entrySet()) {
            final StreamKey key = new StreamKey(carInfo, streamDag.getKey());
            dagDefinition.put(key, elastic);
            for (String transform : streamDag.getValue()) {
                elastic = transforms(key, elastic, transform);
                dagDefinition.put(key, elastic);
            }
        }
    }

    private PCollection<KV<String, ElasticRow>> transforms(StreamKey key, PCollection<KV<String, ElasticRow>> elastic,
                                                           String transformation) {
        final String keyTransform = key.toString() + transformation;
        LOG.info("Applying transformation {}", keyTransform);
        if (transformation.contains("SumColumns")) {
            final SumColumnsKeepParser parser = new SumColumnsKeepParser(transformation);
            return elastic.apply(keyTransform, SumColumnsKeep.as(parser.columnNames(), parser.outputCol()));
        }
        if (transformation.contains("RemoveCol")) {
            final RemoveColParser parser = new RemoveColParser(transformation);
            return elastic.apply(keyTransform, RemoveCol.of(parser.colName()));
        }
        if (transformation.startsWith("FilterByDimension")) {
            return elastic.apply(keyTransform, FilterByDimension.with(transformation));
        }
        if (transformation.startsWith("Upper")) {
            return elastic.apply(keyTransform, UpperDimension.with(transformation));
        }
        return elastic;
    }

}
