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
import org.apache.pulsar.rpc.contrib.client.ReplyListener;

import java.io.IOException;
import java.time.Duration;
import java.util.regex.Pattern;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

@RequiredArgsConstructor
public class MessageDispatcherFactory<REQUEST, REPLY> {
    private final PulsarClient client;
    private final Schema<REQUEST> requestSchema;
    private final Schema<REPLY> replySchema;
    private final String subscription;

    public Producer<REQUEST> requestProducer(ProducerBuilder<REQUEST> requestProducer) throws IOException {
        return requestProducer
                // allow only one client
                .accessMode(ProducerAccessMode.Exclusive)
                .create();
    }

    public Consumer<REPLY> replyConsumer(String topic, ReplyListener<REPLY> listener,
                                         Pattern topicsPattern) throws IOException {
        ConsumerBuilder<REPLY> replyConsumerBuilder = client.newConsumer(replySchema)
                .subscriptionName(subscription)
                // allow only one client
                .subscriptionType(SubscriptionType.Exclusive)
                .messageListener(listener);
        return topicsPattern == null ? replyConsumerBuilder.topic(topic).subscribe()
                : replyConsumerBuilder.topicsPattern(topicsPattern).subscribe();
    }

    public Producer<REPLY> replyProducer(String topic) throws IOException {
        return client
                .newProducer(replySchema)
                .topic(topic)
                // multiple servers can respond to a channel
                .accessMode(ProducerAccessMode.Shared)
                .create();
    }

    public Consumer<REQUEST> requestConsumer(
            String topicsPattern, Duration channelDiscoveryInterval, MessageListener<REQUEST> listener)
            throws IOException {
        return client
                .newConsumer(requestSchema)
                .topicsPattern(topicsPattern)
                // patternAutoDiscoveryPeriod and subscriptionInitialPosition must be set to start consuming
                // from newly created channel request topics within good time and to read from earliest to
                // pick up the first requests
                .patternAutoDiscoveryPeriod((int) channelDiscoveryInterval.toMillis(), MILLISECONDS)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscriptionName(subscription)
                // servers can be horizontally scaled but the same server should receive
                // all messages for the same request
                // Multiple replys per request need to be Key_Shared, but single message
                // requests/reply can just be Shared
                .subscriptionType(SubscriptionType.Key_Shared)
                .messageListener(listener)
                .subscribe();
    }
}
