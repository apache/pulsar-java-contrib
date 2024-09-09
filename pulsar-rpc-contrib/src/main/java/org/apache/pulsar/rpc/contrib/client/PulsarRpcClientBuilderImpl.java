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

import java.time.Duration;
import java.util.Map;
import java.util.regex.Pattern;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.ProducerAccessMode;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.rpc.contrib.common.PulsarRpcClientException;

@Slf4j
@Getter(AccessLevel.PACKAGE)
class PulsarRpcClientBuilderImpl<T, V> implements PulsarRpcClientBuilder<T, V> {
    private final Schema<T> requestSchema;
    private final Schema<V> replySchema;
    private Map<String, Object> requestProducerConfig;
    private String replyTopic;
    private String replySubscription;
    private Duration replyTimeout = Duration.ofSeconds(3);
    private Pattern replyTopicsPattern;
    private Duration patternAutoDiscoveryInterval;
    private RequestCallBack<V> callBack;

    /**
     * Constructs a PulsarRpcClientBuilderImpl with the necessary schemas for request and reply messages.
     *
     * @param requestSchema the schema for serializing request messages
     * @param replySchema the schema for serializing reply messages
     */
    public PulsarRpcClientBuilderImpl(@NonNull Schema<T> requestSchema, @NonNull Schema<V> replySchema) {
        this.requestSchema = requestSchema;
        this.replySchema = replySchema;
    }

    public PulsarRpcClientBuilderImpl<T, V> requestProducerConfig(@NonNull Map<String, Object> requestProducerConfig) {
        this.requestProducerConfig = requestProducerConfig;
        return this;
    }

    public PulsarRpcClientBuilderImpl<T, V> replyTopic(@NonNull String replyTopic) {
        this.replyTopic = replyTopic;
        return this;
    }

    public PulsarRpcClientBuilderImpl<T, V> replyTopicsPattern(@NonNull Pattern replyTopicsPattern) {
        this.replyTopicsPattern = replyTopicsPattern;
        return this;
    }

    public PulsarRpcClientBuilderImpl<T, V> replySubscription(@NonNull String replySubscription) {
        this.replySubscription = replySubscription;
        return this;
    }

    public PulsarRpcClientBuilderImpl<T, V> replyTimeout(@NonNull Duration replyTimeout) {
        this.replyTimeout = replyTimeout;
        return this;
    }

    public PulsarRpcClientBuilderImpl<T, V> patternAutoDiscoveryInterval(
            @NonNull Duration patternAutoDiscoveryInterval) {
        this.patternAutoDiscoveryInterval = patternAutoDiscoveryInterval;
        return this;
    }

    public PulsarRpcClientBuilderImpl<T, V> requestCallBack(@NonNull RequestCallBack<V> callBack) {
        this.callBack = callBack;
        return null;
    }

    public PulsarRpcClientImpl<T, V> build(PulsarClient pulsarClient) throws PulsarRpcClientException {
        if (requestProducerConfig.containsKey("accessMode")
                && requestProducerConfig.get("accessMode") instanceof ProducerAccessMode
                && !requestProducerConfig.get("accessMode").equals(ProducerAccessMode.Exclusive)) {
            throw new PulsarRpcClientException("Producer cannot set the AccessMode to non-Exclusive.");
        }
        return PulsarRpcClientImpl.create(pulsarClient, this);
    }
}
