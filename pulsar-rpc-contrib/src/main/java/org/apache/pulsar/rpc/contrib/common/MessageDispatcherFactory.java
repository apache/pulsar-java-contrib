/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.pulsar.rpc.contrib.common;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import java.io.IOException;
import java.time.Duration;
import java.util.Map;
import java.util.regex.Pattern;
import lombok.RequiredArgsConstructor;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.MessageListener;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerAccessMode;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;

/**
 * Facilitates the creation of producers and consumers for Pulsar based on specified schemas and configurations.
 * This factory simplifies the setup of Pulsar clients for sending and receiving messages in RPC context.
 *
 * @param <T> the type parameter for the request messages.
 * @param <V> the type parameter for the reply messages.
 */
@RequiredArgsConstructor
public class MessageDispatcherFactory<T, V> {
    private final PulsarClient client;
    private final Schema<T> requestSchema;
    private final Schema<V> replySchema;
    private final String subscription;

    /**
     * Creates a Pulsar producer for sending requests. (Pulsar RPC Client side)
     *
     * @param requestProducerConfig the configuration map for request producer.
     * @return the created request message producer.
     * @throws IOException if there is an error creating the producer.
     */
    public Producer<T> requestProducer(Map<String, Object> requestProducerConfig) throws IOException {
        return client.newProducer(requestSchema)
                // allow only one client
                .accessMode(ProducerAccessMode.Exclusive)
                .loadConf(requestProducerConfig)
                .create();
    }

    /**
     * Creates a Pulsar consumer for receiving replies. (Pulsar RPC Client side)
     *
     * @param topic the topic from which to consume messages.
     * @param listener the message listener that handles incoming messages.
     * @param topicsPattern the pattern matching multiple topics for the consumer.
     * @param patternAutoDiscoveryInterval the interval for topic auto-discovery.
     * @return the created reply message consumer.
     * @throws IOException if there is an error creating the consumer.
     */
    public Consumer<V> replyConsumer(String topic,
                                         MessageListener<V> listener,
                                         Pattern topicsPattern,
                                         Duration patternAutoDiscoveryInterval) throws IOException {
        ConsumerBuilder<V> replyConsumerBuilder = client
                .newConsumer(replySchema)
                .patternAutoDiscoveryPeriod((int) patternAutoDiscoveryInterval.toMillis(), MILLISECONDS)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscriptionName(subscription)
                // allow only one client
                .subscriptionType(SubscriptionType.Exclusive)
                .messageListener(listener);
        return topicsPattern == null ? replyConsumerBuilder.topic(topic).subscribe()
                : replyConsumerBuilder.topicsPattern(topicsPattern).subscribe();
    }

    /**
     * Creates a Pulsar consumer for receiving requests. (Pulsar RPC Server side)
     *
     * @param topic the topic from which to consume messages.
     * @param patternAutoDiscoveryInterval the interval for topic auto-discovery.
     * @param listener the message listener that handles incoming messages.
     * @param topicsPattern the pattern matching multiple topics for the consumer.
     * @return the created Consumer.
     * @throws IOException if there is an error creating the consumer.
     */
    public Consumer<T> requestConsumer(
            String topic, Duration patternAutoDiscoveryInterval,
            MessageListener<T> listener, Pattern topicsPattern) throws IOException {
        ConsumerBuilder<T> consumerBuilder = client
                .newConsumer(requestSchema)
                .patternAutoDiscoveryPeriod((int) patternAutoDiscoveryInterval.toMillis(), MILLISECONDS)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscriptionName(subscription)
                .subscriptionType(SubscriptionType.Key_Shared)
                .messageListener(listener);
        return topicsPattern == null ? consumerBuilder.topic(topic).subscribe()
                : consumerBuilder.topicsPattern(topicsPattern).subscribe();
    }

    /**
     * Creates a Pulsar producer for sending replies. (Pulsar RPC Server side)
     *
     * @param topic the topic to which the producer will send messages.
     * @return the created Producer.
     * @throws IOException if there is an error creating the producer.
     */
    public Producer<V> replyProducer(String topic) throws IOException {
        return client
                .newProducer(replySchema)
                .topic(topic)
                .accessMode(ProducerAccessMode.Shared)
                .create();
    }
}
