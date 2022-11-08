package dev.donhk.stream;

import dev.donhk.pojos.UserTxn;
import dev.donhk.utilities.Utils;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.windowing.AfterPane;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Instant;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.stream.Collectors;

@SuppressWarnings("Duplicates")
public class StreamUtils {

    public static PCollection<KV<Long, UserTxn>> UserTxnWindowData(Pipeline pipeline, int _elements, int _windowSize) {
        final PCollection<KV<Long, UserTxn>> input = initializePCollection(pipeline, _elements);
        return input.apply(Window.<KV<Long, UserTxn>>into(new GlobalWindows())
                .triggering(Repeatedly.forever(
                        AfterPane.elementCountAtLeast(_windowSize))
                ).discardingFiredPanes()
                .withOnTimeBehavior(Window.OnTimeBehavior.FIRE_IF_NON_EMPTY));
    }

    private static TestStream.Builder<KV<Long, UserTxn>> createTestStream(
            List<TimestampedValue<KV<Long, UserTxn>>> timestamped) {
        TestStream.Builder<KV<Long, UserTxn>>
                streamBuilder = TestStream.create(UserTxnKVCoder.of());
        for (TimestampedValue<KV<Long, UserTxn>> value : timestamped) {
            streamBuilder = streamBuilder.addElements(value);
        }
        return streamBuilder;
    }

    private static PCollection<KV<Long, UserTxn>> initializePCollection(Pipeline pipeline, int _elements) {
        final List<UserTxn> txn = Utils.getUserTxnList().subList(0, _elements);
        final List<TimestampedValue<KV<Long, UserTxn>>> timestamped
                = createTimeStampedList(txn);
        final TestStream.Builder<KV<Long, UserTxn>> streamBuilder = createTestStream(timestamped);
        return pipeline.apply(streamBuilder.advanceWatermarkToInfinity());
    }

    private static List<TimestampedValue<KV<Long, UserTxn>>> createTimeStampedList(List<UserTxn> txn) {
        return txn.stream().map(i -> {
            final KV<Long, UserTxn> kv = KV.of(i.getId(), i);
            final LocalDateTime time = i.getTime();
            final long millis = time.toInstant(ZoneOffset.UTC).toEpochMilli();
            final Instant instant = new Instant(millis);
            return TimestampedValue.of(kv, instant);
        }).collect(Collectors.toList());
    }
}
