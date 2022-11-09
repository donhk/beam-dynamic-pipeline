package dev.donhk.stream;

import com.google.common.util.concurrent.AtomicDouble;
import dev.donhk.elastict.RemoveCol;
import dev.donhk.elastict.SumColumnsKeep;
import dev.donhk.pojos.Dag;
import dev.donhk.pojos.ElasticRow;
import dev.donhk.pojos.ElasticRowCol;
import dev.donhk.pojos.UserTxn;
import dev.donhk.transform.PrintPCollection;
import dev.donhk.utilities.RemoveColParser;
import dev.donhk.utilities.SumColumnsKeepParser;
import dev.donhk.utilities.Utils;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.joda.time.Instant;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class StreamPipelineBuilder {
    private static final Logger LOG = LogManager.getLogger(StreamPipelineBuilder.class);
    private static final int _windowSize = 10;
    private static final int _elements = 20;

    public void execute() {
        // create the unbounded PCollection from TestStream
        final Pipeline pipeline = Pipeline.create();
        // split into windows
        final PCollection<KV<Long, UserTxn>> windowed = window(pipeline);
        // convert to elastic record
        PCollection<KV<Long, ElasticRow>> elastic = windowed.apply(UserTxn2ElasticRow.of());
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

    private static final class UserTxn2ElasticRow
            extends PTransform<PCollection<KV<Long, UserTxn>>, PCollection<KV<Long, ElasticRow>>> {
        public static UserTxn2ElasticRow of() {
            return new UserTxn2ElasticRow();
        }

        @Override
        public PCollection<KV<Long, ElasticRow>> expand(PCollection<KV<Long, UserTxn>> input) {
            return input.apply(MapElements.into(TypeDescriptors.kvs(
                            TypeDescriptors.longs(),
                            TypeDescriptor.of(ElasticRow.class)))
                    .via((SerializableFunction<KV<Long, UserTxn>, KV<Long, ElasticRow>>) input1 -> {
                                final ElasticRow row = ElasticRow.create();
                                final UserTxn txn = input1.getValue();
                                row.addCol(ElasticRowCol.ID, txn.getId());
                                row.addCol(ElasticRowCol.EMAIL, txn.getEmail());
                                row.addCol(ElasticRowCol.FIRST_NAME, txn.getFirstName());
                                row.addCol(ElasticRowCol.SECOND_NAME, txn.getSecondName());
                                row.addCol(ElasticRowCol.GENDER, txn.getGender());
                                row.addCol(ElasticRowCol.TIME, txn.getTime());
                                row.addCol(ElasticRowCol.AMOUNT, txn.getAmount());
                                row.addCol(ElasticRowCol.MATCH, txn.getMatch());
                                row.addCol(ElasticRowCol.MEMORY, txn.getMemory());
                                return KV.of(input1.getKey(), row);
                            }
                    ));
        }
    }

    private PCollection<KV<Long, UserTxn>> window(Pipeline pipeline) {
        final PCollection<KV<Long, UserTxn>> input =
                initializePCollection(pipeline);
        return input.apply(Window.<KV<Long, UserTxn>>into(new GlobalWindows())
                .triggering(Repeatedly.forever(
                        AfterPane.elementCountAtLeast(_windowSize))
                ).discardingFiredPanes()
                .withOnTimeBehavior(Window.OnTimeBehavior.FIRE_IF_NON_EMPTY));
    }

    private PCollection<KV<Long, UserTxn>> initializePCollection(Pipeline pipeline) {
        final List<UserTxn> txn = Utils.getUserTxnList().subList(0, _elements);
        final List<TimestampedValue<KV<Long, UserTxn>>> timestamped
                = createTimeStampedList(txn);
        final TestStream.Builder<KV<Long, UserTxn>> streamBuilder = createTestStream(timestamped);
        return pipeline.apply(streamBuilder.advanceWatermarkToInfinity());
    }

    private TestStream.Builder<KV<Long, UserTxn>> createTestStream(
            List<TimestampedValue<KV<Long, UserTxn>>> timestamped) {
        TestStream.Builder<KV<Long, UserTxn>>
                streamBuilder = TestStream.create(UserTxnKVCoder.of());
        for (TimestampedValue<KV<Long, UserTxn>> value : timestamped) {
            streamBuilder = streamBuilder.addElements(value);
        }
        return streamBuilder;
    }

    private List<TimestampedValue<KV<Long, UserTxn>>> createTimeStampedList(List<UserTxn> txn) {
        return txn.stream().map(i -> {
            final KV<Long, UserTxn> kv = KV.of(i.getId(), i);
            final LocalDateTime time = i.getTime();
            final long millis = time.toInstant(ZoneOffset.UTC).toEpochMilli();
            final Instant instant = new Instant(millis);
            return TimestampedValue.of(kv, instant);
        }).collect(Collectors.toList());
    }

    private static class PCollectionAggregator
            extends PTransform<PCollection<KV<Long, UserTxn>>, PCollection<KV<Long, Double>>> {
        public static PCollectionAggregator of() {
            return new PCollectionAggregator();
        }

        @Override
        public PCollection<KV<Long, Double>> expand(PCollection<KV<Long, UserTxn>> input) {
            return input.apply(
                    MapElements.into(
                            TypeDescriptors.kvs(TypeDescriptors.longs(), TypeDescriptors.doubles())
                    ).via((record) -> KV.of(record.getKey(), record.getValue().getAmount()))
            ).apply(Combine.globally(KeyAggregator.of()));
        }
    }

    private static class KeyAggregator
            implements SerializableFunction<Iterable<KV<Long, Double>>, KV<Long, Double>> {
        public static KeyAggregator of() {
            return new KeyAggregator();
        }

        @Override
        public KV<Long, Double> apply(Iterable<KV<Long, Double>> input) {
            AtomicLong myLong = new AtomicLong();
            AtomicDouble myDouble = new AtomicDouble();
            StreamSupport.stream(input.spliterator(), false)
                    .forEach(e -> {
                        myLong.addAndGet(e.getKey());
                        myDouble.addAndGet(e.getValue());
                    });
            return KV.of(myLong.get(), myDouble.get());
        }
    }
}
