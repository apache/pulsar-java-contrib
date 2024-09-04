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
package org.apache.pulsar.rpc.contrib.server;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.regex.Pattern;
import lombok.Getter;
import lombok.NonNull;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;

/**
 * Builder class for creating instances of {@link PulsarRpcServer}.
 * This class provides a fluent API to configure the Pulsar RPC server with necessary schemas,
 * topics, subscriptions, and other configuration parameters related to Pulsar clients.
 *
 * <p>Instances of {@link PulsarRpcServer} are configured to handle RPC requests and replies
 * using Apache Pulsar as the messaging system. This builder allows you to specify the request
 * and reply topics, schemas for serialization and deserialization, and other relevant settings.</p>
 *
 * @param <T> the type of the request message
 * @param <V> the type of the reply message
 */
@Getter
public class PulsarRpcServerBuilder<T, V> {
    private final Schema<T> requestSchema;
    private final Schema<V> replySchema;
    private String requestTopic;
    private Pattern requestTopicsPattern;
    private String requestSubscription;
    private Duration patternAutoDiscoveryInterval;

    /**
     * Constructs a new {@link PulsarRpcServerBuilder} with the given request and reply message schemas.
     *
     * @param requestSchema the schema used to serialize request messages
     * @param replySchema the schema used to serialize reply messages
     */
    public PulsarRpcServerBuilder(@NonNull Schema<T> requestSchema, @NonNull Schema<V> replySchema) {
        this.requestSchema = requestSchema;
        this.replySchema = replySchema;
    }

    /**
     * Specifies the Pulsar topic that this server will listen to for receiving requests.
     *
     * @param requestTopic the Pulsar topic name
     * @return this builder instance
     */
    public PulsarRpcServerBuilder<T, V> requestTopic(@NonNull String requestTopic) {
        this.requestTopic = requestTopic;
        return this;
    }

    /**
     * Specifies a pattern for topics that this server will listen to. This is useful for subscribing
     * to multiple topics that match the given pattern.
     *
     * @param requestTopicsPattern the pattern to match topics against
     * @return this builder instance
     */
    public PulsarRpcServerBuilder<T, V> requestTopicsPattern(@NonNull Pattern requestTopicsPattern) {
        this.requestTopicsPattern = requestTopicsPattern;
        return this;
    }

    /**
     * Sets the subscription name for this server to use when subscribing to the request topic.
     *
     * @param requestSubscription the subscription name
     * @return this builder instance
     */
    public PulsarRpcServerBuilder<T, V> requestSubscription(@NonNull String requestSubscription) {
        this.requestSubscription = requestSubscription;
        return this;
    }

    /**
     * Sets the auto-discovery interval for topics. This setting helps in automatically discovering
     * topics that match the set pattern at the specified interval.
     *
     * @param patternAutoDiscoveryInterval the duration to set for auto-discovery
     * @return this builder instance
     */
    public PulsarRpcServerBuilder<T, V> patternAutoDiscoveryInterval(
            @NonNull Duration patternAutoDiscoveryInterval) {
        this.patternAutoDiscoveryInterval = patternAutoDiscoveryInterval;
        return this;
    }

    /**
     * Builds and returns a {@link PulsarRpcServer} instance configured with the current settings of this builder.
     * The server uses provided functional parameters to handle requests and manage rollbacks.
     *
     * @param pulsarClient the client to connect to Pulsar
     * @param requestFunction a function to process incoming requests and generate replies
     * @param rollBackFunction a consumer to handle rollback operations in case of errors
     * @return a new {@link PulsarRpcServer} instance
     * @throws IOException if an error occurs during server initialization
     */
    public PulsarRpcServer<T, V> build(
            PulsarClient pulsarClient, Function<T, CompletableFuture<V>> requestFunction,
            BiConsumer<String, T> rollBackFunction) throws IOException {
        return PulsarRpcServer.create(pulsarClient, requestFunction, rollBackFunction, this);
    }
}
