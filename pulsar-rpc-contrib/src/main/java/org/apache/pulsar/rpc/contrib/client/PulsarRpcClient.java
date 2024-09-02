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

@RequiredArgsConstructor(access = PACKAGE)
public class PulsarRpcClient<T, V> implements AutoCloseable {
    private final ConcurrentHashMap<String, CompletableFuture<V>> pendingRequestsMap;
    private final Duration replyTimeout;
    private final RequestSender<T> sender;
    @Getter
    private final Producer<T> requestProducer;
    private final Consumer<V> replyConsumer;
    private final RequestCallBack<V> callback;

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

        ReplyListener<V> replyListener = new ReplyListener<>(pendingRequestsMap, builder.getCallBack());
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
                builder.getCallBack());
    }

    public static <T, V> PulsarRpcClientBuilder<T, V> builder(
            @NonNull Schema<T> requestSchema, @NonNull Schema<V> replySchema) {
        return new PulsarRpcClientBuilder<>(requestSchema, replySchema);
    }

    @Override
    public void close() throws PulsarClientException {
        try (requestProducer; replyConsumer) {
            pendingRequestsMap.forEach((correlationId, future) -> {
                future.cancel(false);
            });
            pendingRequestsMap.clear();
        }
    }

    public V request(String correlationId, TypedMessageBuilder<T> message) throws Exception {
        return requestAsync(correlationId, message).get();
    }

    public CompletableFuture<V> requestAsync(String correlationId, TypedMessageBuilder<T> message) {
        CompletableFuture<V> replyFuture = new CompletableFuture<>();
        long replyTimeoutMillis = replyTimeout.toMillis();
        replyFuture.orTimeout(replyTimeoutMillis, TimeUnit.MILLISECONDS)
                .exceptionally(e -> {
                    replyFuture.completeExceptionally(e);
                    if (callback != null) {
                        callback.onTimeout(correlationId, e);
                    }
                    removeFailedRequest(correlationId);
                    return null;
                });
        pendingRequestsMap.put(correlationId, replyFuture);
        sender.sendRequest(message, replyTimeoutMillis)
                .thenAccept(requestMessageId -> {
                    if (replyFuture.isCancelled() || replyFuture.isCompletedExceptionally()) {
                        removeFailedRequest(correlationId);
                    } else {
                        if (callback != null) {
                            callback.onSendRequestSuccess(correlationId, requestMessageId);
                        }
                    }
                }).exceptionally(ex -> {
                    if (callback != null) {
                        callback.onSendRequestError(correlationId, ex, replyFuture);
                    } else {
                        replyFuture.completeExceptionally(ex);
                    }
                    removeFailedRequest(correlationId);
                    return null;
                });
        return replyFuture;
    }

    public void removeFailedRequest(String correlationId) {
        pendingRequestsMap.remove(correlationId);
    }

}
