package org.apache.pulsar.client.impl;

import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.client.api.DeadLetterPolicy;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;

public interface DeadLetterPolicyTopicProducerProvider {
    Producer<byte[]> getRetryTopicProducer(DeadLetterPolicy policy, Schema<?> schema) throws PulsarClientException;

    CompletableFuture<Producer<byte[]>> getDLQTopicProducer(DeadLetterPolicy policy, Schema<?> schema);

    /**
     * Indicate if the ConsumerImpl is the producer owner.
     * if yes, the ConsumerImpl should close the created producer.
     */
    boolean isProducerOwner();

    CompletableFuture<Void> closeAsync();
}
