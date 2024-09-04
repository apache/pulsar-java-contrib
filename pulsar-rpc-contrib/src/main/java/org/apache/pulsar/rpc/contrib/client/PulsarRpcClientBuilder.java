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

/**
 * Builder class for constructing a {@link PulsarRpcClient} instance. This builder allows for the
 * customization of various components required to establish a Pulsar RPC client, including
 * schemas for serialization, topic details, and timeout configurations.
 *
 * @param <T> the type of request message
 * @param <V> the type of reply message
 */
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

    /**
     * Constructs a PulsarRpcClientBuilder with the necessary schemas for request and reply messages.
     *
     * @param requestSchema the schema for serializing request messages
     * @param replySchema the schema for serializing reply messages
     */
    public PulsarRpcClientBuilder(@NonNull Schema<T> requestSchema, @NonNull Schema<V> replySchema) {
        this.requestSchema = requestSchema;
        this.replySchema = replySchema;
    }

    /**
     * Specifies the producer builder for request messages.
     *
     * @param requestProducer the producer builder for creating request message producers
     * @return this builder instance for chaining
     */
    public PulsarRpcClientBuilder<T, V> requestProducer(@NonNull ProducerBuilder<T> requestProducer) {
        this.requestProducer = requestProducer;
        return this;
    }

    /**
     * Sets the topic on which reply messages will be sent.
     *
     * @param replyTopic the topic for reply messages
     * @return this builder instance for chaining
     */
    public PulsarRpcClientBuilder<T, V> replyTopic(@NonNull String replyTopic) {
        this.replyTopic = replyTopic;
        return this;
    }

    /**
     * Sets the pattern to subscribe to multiple reply topics dynamically.
     *
     * @param replyTopicsPattern the pattern matching reply topics
     * @return this builder instance for chaining
     */
    public PulsarRpcClientBuilder<T, V> replyTopicsPattern(@NonNull Pattern replyTopicsPattern) {
        this.replyTopicsPattern = replyTopicsPattern;
        return this;
    }

    /**
     * Specifies the subscription name to use for reply messages.
     *
     * @param replySubscription the subscription name for reply messages
     * @return this builder instance for chaining
     */
    public PulsarRpcClientBuilder<T, V> replySubscription(@NonNull String replySubscription) {
        this.replySubscription = replySubscription;
        return this;
    }

    /**
     * Sets the timeout for reply messages.
     *
     * @param replyTimeout the duration to wait for a reply before timing out
     * @return this builder instance for chaining
     */
    public PulsarRpcClientBuilder<T, V> replyTimeout(@NonNull Duration replyTimeout) {
        this.replyTimeout = replyTimeout;
        return this;
    }

    /**
     * Sets the interval for auto-discovery of topics matching the pattern.
     *
     * @param patternAutoDiscoveryInterval the interval for auto-discovering topics
     * @return this builder instance for chaining
     */
    public PulsarRpcClientBuilder<T, V> patternAutoDiscoveryInterval(
            @NonNull Duration patternAutoDiscoveryInterval) {
        this.patternAutoDiscoveryInterval = patternAutoDiscoveryInterval;
        return this;
    }

    /**
     * Sets the {@link RequestCallBack<V>} handler for various request and reply events.
     *
     * @param callBack the callback handler to manage events
     * @return this builder instance for chaining
     */
    public PulsarRpcClientBuilder<T, V> requestCallBack(@NonNull RequestCallBack<V> callBack) {
        this.callBack = callBack;
        return null;
    }

    /**
     * Builds and returns a {@link PulsarRpcClient} configured with the current builder settings.
     *
     * @param pulsarClient the client to use for connecting to server
     * @return a new instance of {@link PulsarRpcClient}
     * @throws IOException if an error occurs during the building of the {@link PulsarRpcClient}
     */
    public PulsarRpcClient<T, V> build(PulsarClient pulsarClient) throws IOException {
        return PulsarRpcClient.create(pulsarClient, this);
    }
}
