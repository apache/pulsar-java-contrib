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
import java.util.regex.Pattern;
import lombok.RequiredArgsConstructor;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.MessageListener;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerAccessMode;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;

@RequiredArgsConstructor
public class MessageDispatcherFactory<T, V> {
    private final PulsarClient client;
    private final Schema<T> requestSchema;
    private final Schema<V> replySchema;
    private final String subscription;

    public Producer<T> requestProducer(ProducerBuilder<T> requestProducer) throws IOException {
        return requestProducer
                // allow only one client
                .accessMode(ProducerAccessMode.Exclusive)
                .create();
    }

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

    public Producer<V> replyProducer(String topic) throws IOException {
        return client
                .newProducer(replySchema)
                .topic(topic)
                .accessMode(ProducerAccessMode.Shared)
                .create();
    }
}
