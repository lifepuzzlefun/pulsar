package org.apache.pulsar.jmh;

import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.bookkeeper.mledger.proto.MLDataFormats;
import org.apache.pulsar.common.util.collections.ConcurrentOpenLongPairRangeSet;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.profile.GCProfiler;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.ArrayList;
import java.util.List;


@Warmup(iterations = 3, time = 60)
@Measurement(iterations = 5, time = 60)
@BenchmarkMode(Mode.Throughput)
@State(Scope.Benchmark)
public class COLPRangeSetBenchmark {

    private final ConcurrentOpenLongPairRangeSet<PositionImpl> set
            = new ConcurrentOpenLongPairRangeSet<>(4096, PositionImpl::new);

    @Param({"1000"})
    public int entryNumber;

    @Param({"50"})
    public int perEntry;

    @Setup
    public void init() {
        for (int i = 0; i < entryNumber; i++) {
            for (int e = 0; e < perEntry; e += 3) {
                set.addOpenClosed(i, e, i, e + 2);
            }
        }
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

        set.forEachWithRangeBoundMapper(
                // conversion function for `ConcurrentOpenLongPairRangeSet`
                (ledgerId, entryId) -> nestedPositionBuilder
                        .setLedgerId(ledgerId)
                        .setEntryId(entryId)
                        .build(),
                // conversion function for `LongPairRangeSet.DefaultRangeSet`
                (positionImpl) -> nestedPositionBuilder
                        .setLedgerId(positionImpl.getLedgerId())
                        .setEntryId(positionImpl.getEntryId())
                        .build(),
                (lowerBound, upperBound) -> {
                    MLDataFormats.MessageRange messageRange = messageRangeBuilder
                            .setLowerEndpoint(lowerBound)
                            .setUpperEndpoint(upperBound).build();

                    rangeList.add(messageRange);

                    return true;

                });

        return rangeList;
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(COLPRangeSetBenchmark.class.getSimpleName())
                .addProfiler(GCProfiler.class)
                .forks(1)
                .resultFormat(ResultFormatType.JSON)
                .build();

        new Runner(opt).run();
    }
}
