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

@Getter
public class PulsarRpcServerBuilder<T, V> {
    private final Schema<T> requestSchema;
    private final Schema<V> replySchema;
    private String requestTopic;
    private Pattern requestTopicsPattern;
    private String requestSubscription;
    private Duration patternAutoDiscoveryInterval;

    public PulsarRpcServerBuilder(@NonNull Schema<T> requestSchema, @NonNull Schema<V> replySchema) {
        this.requestSchema = requestSchema;
        this.replySchema = replySchema;
    }

    public PulsarRpcServerBuilder<T, V> requestTopic(@NonNull String requestTopic) {
        this.requestTopic = requestTopic;
        return this;
    }

    public PulsarRpcServerBuilder<T, V> requestTopicsPattern(@NonNull Pattern requestTopicsPattern) {
        this.requestTopicsPattern = requestTopicsPattern;
        return this;
    }

    public PulsarRpcServerBuilder<T, V> requestSubscription(@NonNull String requestSubscription) {
        this.requestSubscription = requestSubscription;
        return this;
    }

    public PulsarRpcServerBuilder<T, V> patternAutoDiscoveryInterval(
            @NonNull Duration patternAutoDiscoveryInterval) {
        this.patternAutoDiscoveryInterval = patternAutoDiscoveryInterval;
        return this;
    }

    public PulsarRpcServer<T, V> build(
            PulsarClient pulsarClient, Function<T, CompletableFuture<V>> requestFunction,
            BiConsumer<String, T> rollBackFunction) throws IOException {
        return PulsarRpcServer.create(pulsarClient, requestFunction, rollBackFunction, this);
    }
}
