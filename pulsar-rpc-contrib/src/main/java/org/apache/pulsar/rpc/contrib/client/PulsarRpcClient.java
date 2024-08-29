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

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static lombok.AccessLevel.PACKAGE;

@RequiredArgsConstructor(access = PACKAGE)
public class PulsarRpcClient<REQUEST, REPLY> implements AutoCloseable {
    private final ConcurrentHashMap<String, CompletableFuture<REPLY>> pendingRequestsMap;
    private final Duration replyTimeout;
    private final RequestSender<REQUEST> sender;
    @Getter
    private final Producer<REQUEST> requestProducer;
    private final Consumer<REPLY> replyConsumer;

    public static <REQUEST, REPLY> PulsarRpcClient<REQUEST, REPLY> create(
            PulsarClient client, PulsarRpcClientBuilder<REQUEST, REPLY> builder) throws IOException {
        ConcurrentHashMap<String, CompletableFuture<REPLY>> pendingRequestsMap = new ConcurrentHashMap<>();
        MessageDispatcherFactory<REQUEST, REPLY> dispatcherFactory = new MessageDispatcherFactory<>(
                client,
                builder.getRequestSchema(),
                builder.getReplySchema(),
                builder.getReplySubscription());

        Producer<REQUEST> producer = dispatcherFactory.requestProducer(builder.getRequestProducer());
        RequestSender<REQUEST> sender = new RequestSender<>(builder.getReplyTopic());

        ReplyListener<REPLY> replyListener = new ReplyListener<>(pendingRequestsMap);
        Consumer<REPLY> consumer = dispatcherFactory.replyConsumer(
                builder.getReplyTopic(),
                replyListener,
                builder.getReplyTopicsPattern());

        return new PulsarRpcClient<>(
                pendingRequestsMap,
                builder.getReplyTimeout(),
                sender,
                producer,
                consumer
        );
    }

    public static <REQUEST, REPLY> PulsarRpcClientBuilder<REQUEST, REPLY> builder(
            @NonNull Schema<REQUEST> requestSchema, @NonNull Schema<REPLY> replySchema) {
        return new PulsarRpcClientBuilder<>(requestSchema, replySchema);
    }

    @Override
    public void close() throws PulsarClientException {
        try (requestProducer; replyConsumer) {
            pendingRequestsMap.forEach((correlationId, future) -> {
                future.cancel(false);
            });
        }
    }

    public REPLY request(String correlationId, TypedMessageBuilder<REQUEST> message) throws PulsarClientException {
        try {
            return requestAsync(correlationId, message).get();
        } catch (Exception e) {
            throw PulsarClientException.unwrap(e);
        }
    }

    public CompletableFuture<REPLY> requestAsync(String correlationId, TypedMessageBuilder<REQUEST> message) {
        long replyTimeoutMillis = replyTimeout.toMillis();
        CompletableFuture<REPLY> replyFuture = new CompletableFuture<>();
        replyFuture.orTimeout(replyTimeoutMillis, TimeUnit.MILLISECONDS)
                .exceptionally(e -> {
                    replyFuture.completeExceptionally(e);
                    return null;
                });
        pendingRequestsMap.put(correlationId, replyFuture);
        sender.sendRequest(message, replyTimeoutMillis)
                .thenAccept(requestMessageId -> {
                    if (replyFuture.isCompletedExceptionally()) {
                        pendingRequestsMap.remove(correlationId);
                    }
                }).exceptionally(ex -> {
                    replyFuture.completeExceptionally(ex);
                    return null;
                });
        return replyFuture;
    }

}
