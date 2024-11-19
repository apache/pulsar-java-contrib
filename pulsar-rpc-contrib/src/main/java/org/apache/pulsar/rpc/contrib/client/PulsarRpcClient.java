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

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import lombok.NonNull;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.rpc.contrib.common.PulsarRpcClientException;
import org.apache.pulsar.shade.com.google.common.annotations.VisibleForTesting;

/**
 * Provides the functionality to send asynchronous requests and handle replies using Apache Pulsar as the
 * messaging system. This client manages request-response interactions ensuring that messages are sent
 * to the correct topics and handling responses through callbacks.
 *
 * @param <T> The type of the request messages.
 * @param <V> The type of the reply messages.
 */
public interface PulsarRpcClient<T, V> extends AutoCloseable {

    /**
     * Creates a builder for configuring a new {@link PulsarRpcClient}.
     *
     * @return A new instance of {@link PulsarRpcClientBuilder}.
     */
    static <T, V> PulsarRpcClientBuilder<T, V> builder(@NonNull Schema<T> requestSchema,
                                                       @NonNull Schema<V> replySchema) {
        return new PulsarRpcClientBuilderImpl<>(requestSchema, replySchema);
    }

    /**
     * Synchronously sends a request and waits for the replies.
     *
     * @param correlationId A unique identifier for the request.
     * @param value The value used to generate the request message
     * @return The reply value.
     * @throws PulsarRpcClientException if an error occurs during the request or while waiting for the reply.
     */
    default V request(String correlationId, T value) throws PulsarRpcClientException {
        return request(correlationId, value, Collections.emptyMap());
    }

    /**
     * Synchronously sends a request and waits for the replies.
     *
     * @param correlationId A unique identifier for the request.
     * @param value The value used to generate the request message
     * @param config Configuration map for creating a request producer,
     *              will call {@link TypedMessageBuilder#loadConf(Map)}
     * @return The reply value.
     * @throws PulsarRpcClientException if an error occurs during the request or while waiting for the reply.
     */
    V request(String correlationId, T value, Map<String, Object> config) throws PulsarRpcClientException;

    /**
     * Asynchronously sends a request and returns a future that completes with the reply.
     *
     * @param correlationId A unique identifier for the request.
     * @param value The value used to generate the request message
     * @return A CompletableFuture that will complete with the reply value.
     */
    default CompletableFuture<V> requestAsync(String correlationId, T value) {
        return requestAsync(correlationId, value, Collections.emptyMap());
    }

    /**
     * Asynchronously sends a request and returns a future that completes with the reply.
     *
     * @param correlationId A unique identifier for the request.
     * @param value The value used to generate the request message
     * @param config Configuration map for creating a request producer,
     *              will call {@link TypedMessageBuilder#loadConf(Map)}
     * @return A CompletableFuture that will complete with the reply value.
     */
    CompletableFuture<V> requestAsync(String correlationId, T value, Map<String, Object> config);

    /**
     * Removes a request from the tracking map based on its correlation ID.
     *
     * <p>When this method is executed, ReplyListener the received message will not be processed again.
     * You need to make sure that this request has been processed through the callback, or you need to resend it.
     *
     * @param correlationId The correlation ID of the request to remove.
     */
    void removeRequest(String correlationId);

    @VisibleForTesting
    int pendingRequestSize();

    /**
     * Closes this client and releases any resources associated with it. This includes closing any active
     * producers and consumers and clearing pending requests.
     *
     * @throws PulsarRpcClientException if there is an error during the closing process.
     */
    @Override
    void close() throws PulsarRpcClientException;
}
