package org.apache.pulsar.jmh;

import org.apache.bookkeeper.mledger.impl.ManagedCursorImpl;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.bookkeeper.mledger.proto.MLDataFormats;
import org.apache.pulsar.common.util.collections.ConcurrentOpenLongPairRangeSet;
import org.apache.pulsar.common.util.collections.LongPairRangeSet;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.profile.AsyncProfiler;
import org.openjdk.jmh.profile.GCProfiler;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.ArrayList;
import java.util.List;


@Warmup(iterations = 3, time = 30)
@Measurement(iterations = 5, time = 30)
@BenchmarkMode(Mode.Throughput)
@State(Scope.Benchmark)
public class COLPRangeSetBenchmark {

    private final ConcurrentOpenLongPairRangeSet<PositionImpl> set
            = new ConcurrentOpenLongPairRangeSet<>(4096, PositionImpl::new);

    private final LongPairRangeSet.DefaultRangeSet<PositionImpl> defaultRangeSetNeedRecycle
            = new LongPairRangeSet.DefaultRangeSet<>(PositionImpl::new,
            ManagedCursorImpl.positionRangeReverseConverter,
            true);

    private final LongPairRangeSet.DefaultRangeSet<PositionImpl> defaultRangeSetNoRecycle
            = new LongPairRangeSet.DefaultRangeSet<>(PositionImpl::new,
            (position) -> new LongPairRangeSet.LongPair(position.getLedgerId(),position.getEntryId()),
            false);

    @Param({"1000"})
    public int entryNumber;

    @Param({"50"})
    public int perEntry;

    @Setup
    public void init() {
        for (int i = 0; i < entryNumber; i++) {
            for (int e = 0; e < perEntry; e += 3) {
                set.addOpenClosed(i, e, i, e + 2);
                defaultRangeSetNeedRecycle.addOpenClosed(i, e, i, e + 2);
                defaultRangeSetNoRecycle.addOpenClosed(i, e, i, e + 2);
            }
        }
    }


    @Benchmark
    public List<MLDataFormats.MessageRange> defaultRecycle() {
        List<MLDataFormats.MessageRange> rangeList = new ArrayList<>(set.size());
        MLDataFormats.NestedPositionInfo.Builder nestedPositionBuilder = MLDataFormats.NestedPositionInfo
                .newBuilder();
        MLDataFormats.MessageRange.Builder messageRangeBuilder = MLDataFormats.MessageRange.newBuilder();

        defaultRangeSetNeedRecycle.forEachRawRange((lowerKey, lowerValue, upperKey, upperValue) -> {
            MLDataFormats.NestedPositionInfo lowerPosition = nestedPositionBuilder
                    .setLedgerId(lowerKey)
                    .setEntryId(lowerValue)
                    .build();

            MLDataFormats.NestedPositionInfo upperPosition = nestedPositionBuilder
                    .setLedgerId(lowerKey)
                    .setEntryId(lowerValue)
                    .build();

            MLDataFormats.MessageRange messageRange = messageRangeBuilder
                    .setLowerEndpoint(lowerPosition)
                    .setUpperEndpoint(upperPosition)
                    .build();

            rangeList.add(messageRange);

            return true;
        });

        return rangeList;
    }

    @Benchmark
    public List<MLDataFormats.MessageRange> defaultOldNoRecycle() {
        List<MLDataFormats.MessageRange> rangeList = new ArrayList<>(set.size());
        MLDataFormats.NestedPositionInfo.Builder nestedPositionBuilder = MLDataFormats.NestedPositionInfo
                .newBuilder();
        MLDataFormats.MessageRange.Builder messageRangeBuilder = MLDataFormats.MessageRange.newBuilder();

        defaultRangeSetNoRecycle.forEachRawRange((lowerKey, lowerValue, upperKey, upperValue) -> {
            MLDataFormats.NestedPositionInfo lowerPosition = nestedPositionBuilder
                    .setLedgerId(lowerKey)
                    .setEntryId(lowerValue)
                    .build();

            MLDataFormats.NestedPositionInfo upperPosition = nestedPositionBuilder
                    .setLedgerId(lowerKey)
                    .setEntryId(lowerValue)
                    .build();

            MLDataFormats.MessageRange messageRange = messageRangeBuilder
                    .setLowerEndpoint(lowerPosition)
                    .setUpperEndpoint(upperPosition)
                    .build();

            rangeList.add(messageRange);

            return true;
        });

        return rangeList;
    }

    @Benchmark
    public List<MLDataFormats.MessageRange> old() {
        List<MLDataFormats.MessageRange> rangeList = new ArrayList<>(set.size());
        MLDataFormats.NestedPositionInfo.Builder nestedPositionBuilder = MLDataFormats.NestedPositionInfo
                .newBuilder();
        MLDataFormats.MessageRange.Builder messageRangeBuilder = MLDataFormats.MessageRange.newBuilder();

        set.forEach((positionRange) -> {
            PositionImpl p = positionRange.lowerEndpoint();
            nestedPositionBuilder.setLedgerId(p.getLedgerId());
            nestedPositionBuilder.setEntryId(p.getEntryId());
            messageRangeBuilder.setLowerEndpoint(nestedPositionBuilder.build());
            p = positionRange.upperEndpoint();
            nestedPositionBuilder.setLedgerId(p.getLedgerId());
            nestedPositionBuilder.setEntryId(p.getEntryId());
            messageRangeBuilder.setUpperEndpoint(nestedPositionBuilder.build());
            MLDataFormats.MessageRange messageRange = messageRangeBuilder.build();
            rangeList.add(messageRange);
            return true;
        });
        return rangeList;
    }

    @Benchmark
    public List<MLDataFormats.MessageRange> newImpl() {
        List<MLDataFormats.MessageRange> rangeList = new ArrayList<>(set.size());
        MLDataFormats.NestedPositionInfo.Builder nestedPositionBuilder = MLDataFormats.NestedPositionInfo
                .newBuilder();
        MLDataFormats.MessageRange.Builder messageRangeBuilder = MLDataFormats.MessageRange.newBuilder();

        set.forEachRawRange((lowerKey, lowerValue, upperKey, upperValue) -> {
            MLDataFormats.NestedPositionInfo lowerPosition = nestedPositionBuilder
                    .setLedgerId(lowerKey)
                    .setEntryId(lowerValue)
                    .build();

            MLDataFormats.NestedPositionInfo upperPosition = nestedPositionBuilder
                    .setLedgerId(lowerKey)
                    .setEntryId(lowerValue)
                    .build();

            MLDataFormats.MessageRange messageRange = messageRangeBuilder
                    .setLowerEndpoint(lowerPosition)
                    .setUpperEndpoint(upperPosition)
                    .build();

            rangeList.add(messageRange);

            return true;
        });

        return rangeList;
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(COLPRangeSetBenchmark.class.getSimpleName())
                .addProfiler(GCProfiler.class)
                .addProfiler("jfr")
//                .addProfiler(AsyncProfiler.class, "")
                .forks(1)
                .resultFormat(ResultFormatType.JSON)
                .build();

        new Runner(opt).run();
    }
}
