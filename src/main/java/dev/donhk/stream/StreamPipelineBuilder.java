package dev.donhk.stream;

import dev.donhk.pojos.UserTxn;
import dev.donhk.transform.PrintPCollection;
import dev.donhk.utilities.Utils;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Duration;
import org.joda.time.Instant;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.stream.Collectors;

public class StreamPipelineBuilder {
    public void execute() {
        final List<UserTxn> txn = Utils.getUserTxnList();
        // create Pipeline
        final Pipeline pipeline = Pipeline.create();
        TestStream.Builder<KV<Long, UserTxn>>
                streamBuilder = TestStream.create(UserTxnKVCoder.of());
        // add all lines with timestamps to the TestStream
        final List<TimestampedValue<KV<Long, UserTxn>>> timestamped =
                txn.stream().map(i -> {
                    final KV<Long, UserTxn> kv = KV.of(i.getId(), i);
                    final LocalDateTime time = i.getTime();
                    final long millis = time.toInstant(ZoneOffset.UTC).toEpochMilli();
                    final Instant instant = new Instant(millis);
                    return TimestampedValue.of(kv, instant);
                }).collect(Collectors.toList());

        for (TimestampedValue<KV<Long, UserTxn>> value : timestamped) {
            streamBuilder = streamBuilder.addElements(value);
        }

        // create the unbounded PCollection from TestStream
        PCollection<KV<Long, UserTxn>> input = pipeline.apply(streamBuilder.advanceWatermarkToInfinity());
        PCollection<KV<Long, UserTxn>> windowed =
                input.apply(Window.<KV<Long, UserTxn>>into(FixedWindows.of(Duration.standardSeconds(1)))
                        .discardingFiredPanes()
                        .triggering(AfterPane.elementCountAtLeast(1))
                        .withAllowedLateness(Duration.ZERO));

        windowed.apply(PrintPCollection.with());

        pipeline.run().waitUntilFinish();
    }
}
