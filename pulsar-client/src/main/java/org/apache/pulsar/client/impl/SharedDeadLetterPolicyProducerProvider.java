package org.apache.pulsar.client.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.client.api.DeadLetterPolicy;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.util.FutureUtil;

public class SharedDeadLetterPolicyProducerProvider implements DeadLetterPolicyTopicProducerProvider {
    private final PulsarClientImpl client;
    private final Map<Pair<DeadLetterPolicy, Schema<?>>, Producer<byte[]>> sharedRetryProducer;
    private final Map<Pair<DeadLetterPolicy, Schema<?>>, CompletableFuture<Producer<byte[]>>> sharedDLQProducer;

    public SharedDeadLetterPolicyProducerProvider(PulsarClientImpl client) {
        this.client = client;
        this.sharedRetryProducer = new ConcurrentHashMap<>();
        this.sharedDLQProducer = new ConcurrentHashMap<>();
    }

    @Override
    public synchronized Producer<byte[]> getRetryTopicProducer(DeadLetterPolicy policy,
                                                               Schema<?> schema)
            throws PulsarClientException {
        Pair<DeadLetterPolicy, Schema<?>> pair = Pair.of(policy, schema);

        if (sharedDLQProducer.get(pair) != null) {
            return sharedRetryProducer.get(pair);
        }

        Producer<byte[]> producer = client
                .newProducer(Schema.AUTO_PRODUCE_BYTES(schema))
                .topic(policy.getRetryLetterTopic())
                .enableBatching(false)
                .enableChunking(true)
                .blockIfQueueFull(false)
                .create();

        sharedRetryProducer.put(pair, producer);

        return producer;
    }

    @Override
    public synchronized CompletableFuture<Producer<byte[]>> getDLQTopicProducer(DeadLetterPolicy policy,
                                                                                Schema<?> schema) {
        Pair<DeadLetterPolicy, Schema<?>> pair = Pair.of(policy, schema);

        if (sharedDLQProducer.get(pair) != null) {
            return sharedDLQProducer.get(pair);
        }

        CompletableFuture<Producer<byte[]>> producerFuture =
                ((ProducerBuilderImpl<byte[]>) client.newProducer(Schema.AUTO_PRODUCE_BYTES(schema)))
                        .initialSubscriptionName(policy.getInitialSubscriptionName())
                        .topic(policy.getDeadLetterTopic())
                        .blockIfQueueFull(false)
                        .enableBatching(false)
                        .enableChunking(true)
                        .createAsync();

        producerFuture.whenComplete((producer, e) -> {
            sharedDLQProducer.remove(pair);
        });

        sharedDLQProducer.put(pair, producerFuture);

        return producerFuture;
    }

    @Override
    public boolean isProducerOwner() {
        return false;
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        List<CompletableFuture<Void>> futureList = new ArrayList<>();

        sharedRetryProducer.values().stream().map(Producer::closeAsync).forEach(futureList::add);

        sharedDLQProducer.values().forEach(future -> futureList.add(future.thenCompose(Producer::closeAsync)));

        return FutureUtil.waitForAll(futureList);
    }
}
