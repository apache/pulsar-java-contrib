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

import static lombok.AccessLevel.PACKAGE;
import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.rpc.contrib.common.MessageDispatcherFactory;

/**
 * Provides the functionality to send asynchronous requests and handle replies using Apache Pulsar as the
 * messaging system. This client manages request-response interactions ensuring that messages are sent
 * to the correct topics and handling responses through callbacks.
 *
 * @param <T> The type of the request messages.
 * @param <V> The type of the reply messages.
 */
@RequiredArgsConstructor(access = PACKAGE)
public class PulsarRpcClient<T, V> implements AutoCloseable {
    private final ConcurrentHashMap<String, CompletableFuture<V>> pendingRequestsMap;
    private final Duration replyTimeout;
    private final RequestSender<T> sender;
    @Getter
    private final Producer<T> requestProducer;
    private final Consumer<V> replyConsumer;
    private final RequestCallBack<V> callback;

    /**
     * Creates a new instance of {@link PulsarRpcClient} using the specified builder settings.
     *
     * @param client The Pulsar client to use for creating producers and consumers.
     * @param builder The builder containing configurations for the client.
     * @return A new instance of {@link PulsarRpcClient}.
     * @throws IOException if there is an error during the client initialization.
     */
    public static <T, V> PulsarRpcClient<T, V> create(
            @NonNull PulsarClient client, @NonNull PulsarRpcClientBuilder<T, V> builder) throws IOException {
        ConcurrentHashMap<String, CompletableFuture<V>> pendingRequestsMap = new ConcurrentHashMap<>();
        MessageDispatcherFactory<T, V> dispatcherFactory = new MessageDispatcherFactory<>(
                client,
                builder.getRequestSchema(),
                builder.getReplySchema(),
                builder.getReplySubscription());

        Producer<T> producer = dispatcherFactory.requestProducer(builder.getRequestProducer());
        RequestSender<T> sender = new RequestSender<>(null == builder.getReplyTopic()
                ? builder.getReplyTopicsPattern().pattern() : builder.getReplyTopic());

        RequestCallBack<V> callBack = builder.getCallBack() == null ? new DefaultRequestCallBack<>()
                : builder.getCallBack();

        ReplyListener<V> replyListener = new ReplyListener<>(pendingRequestsMap, callBack);
        Consumer<V> consumer = dispatcherFactory.replyConsumer(
                builder.getReplyTopic(),
                replyListener,
                builder.getReplyTopicsPattern(),
                builder.getPatternAutoDiscoveryInterval());

        return new PulsarRpcClient<>(
                pendingRequestsMap,
                builder.getReplyTimeout(),
                sender,
                producer,
                consumer,
                callBack);
    }

    /**
     * Creates a builder for configuring a new {@link PulsarRpcClient}.
     *
     * @param requestSchema The schema for request messages.
     * @param replySchema The schema for reply messages.
     * @return A new instance of {@link PulsarRpcClientBuilder}.
     */
    public static <T, V> PulsarRpcClientBuilder<T, V> builder(
            @NonNull Schema<T> requestSchema, @NonNull Schema<V> replySchema) {
        return new PulsarRpcClientBuilder<>(requestSchema, replySchema);
    }

    /**
     * Closes this client and releases any resources associated with it. This includes closing any active
     * producers and consumers and clearing pending requests.
     *
     * @throws PulsarClientException if there is an error during the closing process.
     */
    @Override
    public void close() throws PulsarClientException {
        try (requestProducer; replyConsumer) {
            pendingRequestsMap.forEach((correlationId, future) -> {
                future.cancel(false);
            });
            pendingRequestsMap.clear();
        }
    }

    /**
     * Synchronously sends a request and waits for the response.
     *
     * @param correlationId A unique identifier for the request.
     * @param message The message builder used to build and send the request.
     * @return The reply value.
     * @throws Exception if an error occurs during the request or while waiting for the reply.
     */
    public V request(String correlationId, TypedMessageBuilder<T> message) throws Exception {
        return requestAsync(correlationId, message).get();
    }

    /**
     * Asynchronously sends a request and returns a future that completes with the reply.
     *
     * @param correlationId A unique identifier for the request.
     * @param message The message builder used to build and send the request.
     * @return A CompletableFuture that will complete with the reply value.
     */
    public CompletableFuture<V> requestAsync(String correlationId, TypedMessageBuilder<T> message) {
        CompletableFuture<V> replyFuture = new CompletableFuture<>();
        long replyTimeoutMillis = replyTimeout.toMillis();
        replyFuture.orTimeout(replyTimeoutMillis, TimeUnit.MILLISECONDS)
                .exceptionally(e -> {
                    replyFuture.completeExceptionally(e);
                    callback.onTimeout(correlationId, e);
                    removeRequest(correlationId);
                    return null;
                });
        pendingRequestsMap.put(correlationId, replyFuture);
        sender.sendRequest(message, replyTimeoutMillis)
                .thenAccept(requestMessageId -> {
                    if (replyFuture.isCancelled() || replyFuture.isCompletedExceptionally()) {
                        removeRequest(correlationId);
                    } else {
                        callback.onSendRequestSuccess(correlationId, requestMessageId);
                    }
                }).exceptionally(ex -> {
                    if (callback != null) {
                        callback.onSendRequestError(correlationId, ex, replyFuture);
                    } else {
                        replyFuture.completeExceptionally(ex);
                    }
                    removeRequest(correlationId);
                    return null;
                });
        return replyFuture;
    }

    /**
     * Removes a request from the tracking map based on its correlation ID.
     *
     * <p>When this method is executed, ReplyListener the received message will not be processed again.
     * You need to make sure that this request has been processed through the callback, or you need to resend it.
     *
     * @param correlationId The correlation ID of the request to remove.
     */
    public void removeRequest(String correlationId) {
        pendingRequestsMap.remove(correlationId);
    }

}