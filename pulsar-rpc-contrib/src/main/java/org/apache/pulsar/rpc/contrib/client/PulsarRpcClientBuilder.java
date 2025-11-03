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
import lombok.NonNull;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.rpc.contrib.common.PulsarRpcClientException;

/**
 * Builder class for constructing a {@link PulsarRpcClient} instance. This builder allows for the
 * customization of various components required to establish a Pulsar RPC client, including schemas
 * for serialization, topic details, and timeout configurations.
 *
 * @param <T> the type of request message
 * @param <V> the type of reply message
 */
public interface PulsarRpcClientBuilder<T, V> {

  /**
   * Specifies the Pulsar topic that this client will send to for requests.
   *
   * @param requestTopic the Pulsar topic name
   * @return this builder instance for chaining
   */
  PulsarRpcClientBuilder<T, V> requestTopic(String requestTopic);

  /**
   * Specifies the producer configuration map for request messages.
   *
   * @param requestProducerConfig Configuration map for creating a request message producer, will
   *     call {@link org.apache.pulsar.client.api.ProducerBuilder#loadConf(java.util.Map)}
   * @return this builder instance for chaining
   */
  PulsarRpcClientBuilder<T, V> requestProducerConfig(
      @NonNull Map<String, Object> requestProducerConfig);

  /**
   * Sets the topic on which reply messages will be sent.
   *
   * @param replyTopic the topic for reply messages
   * @return this builder instance for chaining
   */
  PulsarRpcClientBuilder<T, V> replyTopic(@NonNull String replyTopic);

  /**
   * Sets the pattern to subscribe to multiple reply topics dynamically.
   *
   * @param replyTopicsPattern the pattern matching reply topics
   * @return this builder instance for chaining
   */
  PulsarRpcClientBuilder<T, V> replyTopicsPattern(@NonNull Pattern replyTopicsPattern);

  /**
   * Specifies the subscription name to use for reply messages.
   *
   * @param replySubscription the subscription name for reply messages
   * @return this builder instance for chaining
   */
  PulsarRpcClientBuilder<T, V> replySubscription(@NonNull String replySubscription);

  /**
   * Sets the timeout for reply messages.
   *
   * @param replyTimeout the duration to wait for a reply before timing out
   * @return this builder instance for chaining
   */
  PulsarRpcClientBuilder<T, V> replyTimeout(@NonNull Duration replyTimeout);

  /**
   * Sets the interval for auto-discovery of topics matching the pattern.
   *
   * @param patternAutoDiscoveryInterval the interval for auto-discovering topics
   * @return this builder instance for chaining
   */
  PulsarRpcClientBuilder<T, V> patternAutoDiscoveryInterval(
      @NonNull Duration patternAutoDiscoveryInterval);

  /**
   * Sets the {@link RequestCallBack<V>} handler for various request and reply events.
   *
   * @param callBack the callback handler to manage events
   * @return this builder instance for chaining
   */
  PulsarRpcClientBuilder<T, V> requestCallBack(@NonNull RequestCallBack<V> callBack);

  /**
   * Builds and returns a {@link PulsarRpcClient} configured with the current builder settings.
   *
   * @param pulsarClient the client to use for connecting to server
   * @return a new instance of {@link PulsarRpcClient}
   * @throws PulsarRpcClientException if an error occurs during the building of the {@link
   *     PulsarRpcClient}
   */
  PulsarRpcClient<T, V> build(PulsarClient pulsarClient) throws PulsarRpcClientException;
}
