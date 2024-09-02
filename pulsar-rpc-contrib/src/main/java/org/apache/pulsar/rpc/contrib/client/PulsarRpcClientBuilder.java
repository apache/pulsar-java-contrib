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
package org.apache.pulsar.rpc.contrib.client;

import java.io.IOException;
import java.time.Duration;
import java.util.regex.Pattern;
import lombok.Getter;
import lombok.NonNull;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;

@Getter
public class PulsarRpcClientBuilder<T, V> {
    private final Schema<T> requestSchema;
    private final Schema<V> replySchema;
    private ProducerBuilder<T> requestProducer;
    private String replyTopic;
    private String replySubscription;
    private Duration replyTimeout = Duration.ofSeconds(3);
    private Pattern replyTopicsPattern;
    private Duration patternAutoDiscoveryInterval;
    private RequestCallBack<V> callBack;

    public PulsarRpcClientBuilder(@NonNull Schema<T> requestSchema, @NonNull Schema<V> replySchema) {
        this.requestSchema = requestSchema;
        this.replySchema = replySchema;
    }

    public PulsarRpcClientBuilder<T, V> requestProducer(@NonNull ProducerBuilder<T> requestProducer) {
        this.requestProducer = requestProducer;
        return this;
    }

    public PulsarRpcClientBuilder<T, V> replyTopic(@NonNull String replyTopic) {
        this.replyTopic = replyTopic;
        return this;
    }

    public PulsarRpcClientBuilder<T, V> replyTopicsPattern(@NonNull Pattern replyTopicsPattern) {
        this.replyTopicsPattern = replyTopicsPattern;
        return this;
    }

    public PulsarRpcClientBuilder<T, V> replySubscription(@NonNull String replySubscription) {
        this.replySubscription = replySubscription;
        return this;
    }

    public PulsarRpcClientBuilder<T, V> replyTimeout(@NonNull Duration replyTimeout) {
        this.replyTimeout = replyTimeout;
        return this;
    }

    public PulsarRpcClientBuilder<T, V> patternAutoDiscoveryInterval(
            @NonNull Duration patternAutoDiscoveryInterval) {
        this.patternAutoDiscoveryInterval = patternAutoDiscoveryInterval;
        return this;
    }

    public PulsarRpcClientBuilder<T, V> requestCallBack(@NonNull RequestCallBack<V> callBack) {
        this.callBack = callBack;
        return null;
    }

    public PulsarRpcClient<T, V> build(PulsarClient pulsarClient) throws IOException {
        return PulsarRpcClient.create(pulsarClient, this);
    }


}
