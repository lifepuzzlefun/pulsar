package org.apache.pulsar;


import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.impl.EntryImpl;
import org.apache.pulsar.broker.service.EntryAndMetadata;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;

@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
@Warmup(iterations = 5, time = 5)
@Measurement(iterations = 5, time = 10)
public class OptionalCompareWithIfCondition {

    public static final MessageMetadata data = new MessageMetadata();

    public static final MessageMetadata[] NULL = null;
    public static final Optional<MessageMetadata[]> EMPTY = Optional.empty();

    public static MessageMetadata[] metadataArray;
    public static Optional<MessageMetadata[]> optionalMessageMetadata;
    public static final EntryAndMetadata entryAndMetadata = EntryAndMetadata.create(null, data);

    public static final Entry entry = EntryImpl.create(0,0,new byte[0]);

    public int metadataIndex;

    @Setup(Level.Iteration)
    public void init() {
        metadataIndex = ThreadLocalRandom.current().nextInt(100);
    }

    @Setup
    public void prepareMetadataArray() {
        metadataArray = new MessageMetadata[100];

        for (int i = 0; i < 100; i++) {
            metadataArray[i] = new MessageMetadata();
        }

        optionalMessageMetadata = Optional.of(metadataArray);
    }


    public MessageMetadata getMessageMetadataAt(MessageMetadata[] metadataArray, int metadataIndex, Entry entry) {
        MessageMetadata msgMetadata;
        if (metadataArray != null) {
            msgMetadata = metadataArray[metadataIndex];
        } else if (entry instanceof EntryAndMetadata entryAndMetadata) {
            msgMetadata = entryAndMetadata.getMetadata();
        } else {
            msgMetadata = data;
        }

        return msgMetadata;
    }


    // 1. not null
    // 2. null and entryAndMetadata
    // 3. other


    public MessageMetadata getMessageMetadataAtOptional(Optional<MessageMetadata[]> optMetadataArray, int metadataIndex, Entry entry) {
        return optMetadataArray.map(metadataArray -> metadataArray[metadataIndex]).orElseGet(() -> (entry instanceof EntryAndMetadata) ? ((EntryAndMetadata) entry).getMetadata() : data);
    }

    // case 1

    @Benchmark
    public MessageMetadata GetIfCondition() {
        return getMessageMetadataAt(metadataArray, metadataIndex, entryAndMetadata);
    }

    @Benchmark
    public MessageMetadata GetOptional() {
        return getMessageMetadataAtOptional(optionalMessageMetadata, metadataIndex, entryAndMetadata);
    }

    // case 2

    @Benchmark
    public MessageMetadata EmptyAndEntryAndMetadataIfCondition() {
        return getMessageMetadataAt(NULL, metadataIndex, entryAndMetadata);
    }

    @Benchmark
    public MessageMetadata EmptyAndEntryAndMetadataOptional() {
        return getMessageMetadataAtOptional(EMPTY, metadataIndex, entryAndMetadata);
    }

    // case 3

    @Benchmark
    public MessageMetadata EmptyAndNotEntryMetadataIfCondition() {
        return getMessageMetadataAt(NULL, metadataIndex, entry);
    }

    @Benchmark
    public MessageMetadata EmptyAndNotEntryMetadataOptional() {
        return getMessageMetadataAtOptional(EMPTY, metadataIndex, entry);
    }


    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(OptionalCompareWithIfCondition.class.getSimpleName())
                .forks(1)
                .resultFormat(ResultFormatType.JSON)
                .build();

        new Runner(opt).run();
    }

}
