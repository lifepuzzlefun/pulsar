package org.apache.pulsar.client.impl;

import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.client.api.DeadLetterPolicy;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;

public class DefaultDeadLetterPolicyProducerProvider implements DeadLetterPolicyTopicProducerProvider {
    private final PulsarClientImpl client;

    public DefaultDeadLetterPolicyProducerProvider(PulsarClientImpl client) {
        this.client = client;
    }

    @Override
    public Producer<byte[]> getRetryTopicProducer(DeadLetterPolicy deadLetterPolicy, Schema<?> schema)
            throws PulsarClientException {
        return client
                .newProducer(Schema.AUTO_PRODUCE_BYTES(schema))
                .topic(deadLetterPolicy.getRetryLetterTopic())
                .enableBatching(false)
                .enableChunking(true)
                .blockIfQueueFull(false)
                .create();
    }

    @Override
    public CompletableFuture<Producer<byte[]>> getDLQTopicProducer(DeadLetterPolicy deadLetterPolicy,
                                                                   Schema<?> schema) {
        return ((ProducerBuilderImpl<byte[]>) client.newProducer(Schema.AUTO_PRODUCE_BYTES(schema)))
                .initialSubscriptionName(deadLetterPolicy.getInitialSubscriptionName())
                .topic(deadLetterPolicy.getDeadLetterTopic())
                .blockIfQueueFull(false)
                .enableBatching(false)
                .enableChunking(true)
                .createAsync();
    }

    @Override
    public boolean isProducerOwner() {
        return true;
    }

    public CompletableFuture<Void> closeAsync() {
        return CompletableFuture.completedFuture(null);
    }
}
