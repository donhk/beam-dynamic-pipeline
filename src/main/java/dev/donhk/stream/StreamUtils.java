package dev.donhk.stream;

import dev.donhk.pojos.CarInformation;
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

    public static PCollection<KV<String, CarInformation>> carInfoWindowData(Pipeline pipeline, int _elements, int _windowSize) {
        final List<CarInformation> txn = Utils.getCarInfoList().subList(0, _elements);
        final List<TimestampedValue<KV<String, CarInformation>>> timestamped = txn.stream().map(i -> {
            final KV<String, CarInformation> kv = KV.of(String.valueOf(i.getId()), i);
            final LocalDateTime time = i.getCarTime();
            final long millis = time.toInstant(ZoneOffset.UTC).toEpochMilli();
            final Instant instant = new Instant(millis);
            return TimestampedValue.of(kv, instant);
        }).collect(Collectors.toList());

        TestStream.Builder<KV<String, CarInformation>> streamBuilder =
                TestStream.create(CarInfoKVCoder.of());
        for (TimestampedValue<KV<String, CarInformation>> value : timestamped) {
            streamBuilder = streamBuilder.addElements(value);
        }

        return pipeline.apply(streamBuilder.advanceWatermarkToInfinity())
                .apply("car-info-window", Window.<KV<String, CarInformation>>into(new GlobalWindows())
                        .triggering(Repeatedly.forever(
                                AfterPane.elementCountAtLeast(_windowSize))
                        ).discardingFiredPanes()
                        .withOnTimeBehavior(Window.OnTimeBehavior.FIRE_IF_NON_EMPTY));
    }

    public static PCollection<KV<String, UserTxn>> userTxnWindowData(Pipeline pipeline, int _elements, int _windowSize) {
        final PCollection<KV<String, UserTxn>> input = initializePCollection(pipeline, _elements);
        return input.apply("user-txn-window", Window.<KV<String, UserTxn>>into(new GlobalWindows())
                .triggering(Repeatedly.forever(
                        AfterPane.elementCountAtLeast(_windowSize))
                ).discardingFiredPanes()
                .withOnTimeBehavior(Window.OnTimeBehavior.FIRE_IF_NON_EMPTY));
    }

    private static TestStream.Builder<KV<String, UserTxn>> createTestStream(List<TimestampedValue<KV<String, UserTxn>>> timestamped) {
        TestStream.Builder<KV<String, UserTxn>>
                streamBuilder = TestStream.create(UserTxnKVCoder.of());
        for (TimestampedValue<KV<String, UserTxn>> value : timestamped) {
            streamBuilder = streamBuilder.addElements(value);
        }
        return streamBuilder;
    }

    private static PCollection<KV<String, UserTxn>> initializePCollection(Pipeline pipeline, int _elements) {
        final List<UserTxn> txn = Utils.getUserTxnList().subList(0, _elements);
        final List<TimestampedValue<KV<String, UserTxn>>> timestamped
                = createTimeStampedList(txn);
        final TestStream.Builder<KV<String, UserTxn>> streamBuilder = createTestStream(timestamped);
        return pipeline.apply(streamBuilder.advanceWatermarkToInfinity());
    }

    private static List<TimestampedValue<KV<String, UserTxn>>> createTimeStampedList(List<UserTxn> txn) {
        return txn.stream().map(i -> {
            final KV<String, UserTxn> kv = KV.of(String.valueOf(i.getId()), i);
            final LocalDateTime time = i.getTime();
            final long millis = time.toInstant(ZoneOffset.UTC).toEpochMilli();
            final Instant instant = new Instant(millis);
            return TimestampedValue.of(kv, instant);
        }).collect(Collectors.toList());
    }
}
