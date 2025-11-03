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
import static org.apache.pulsar.rpc.contrib.common.Constants.REQUEST_DELIVER_AT_TIME;
import java.io.IOException;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.common.util.ExceptionHandler;
import org.apache.pulsar.rpc.contrib.common.MessageDispatcherFactory;
import org.apache.pulsar.rpc.contrib.common.PulsarRpcClientException;
import org.apache.pulsar.shade.com.google.common.annotations.VisibleForTesting;

@RequiredArgsConstructor(access = PACKAGE)
class PulsarRpcClientImpl<T, V> implements PulsarRpcClient<T, V> {
  private final ConcurrentHashMap<String, CompletableFuture<V>> pendingRequestsMap;
  private final Duration replyTimeout;
  private final RequestSender<T> sender;
  @Getter private final Producer<T> requestProducer;
  private final Consumer<V> replyConsumer;
  private final RequestCallBack<V> callback;

  /**
   * Creates a new instance of {@link PulsarRpcClientImpl} using the specified builder settings.
   *
   * @param client The Pulsar client to use for creating producers and consumers.
   * @param builder The builder containing configurations for the client.
   * @return A new instance of {@link PulsarRpcClientImpl}.
   * @throws PulsarRpcClientException if there is an error during the client initialization.
   */
  static <T, V> PulsarRpcClientImpl<T, V> create(
      @NonNull PulsarClient client, @NonNull PulsarRpcClientBuilderImpl<T, V> builder)
      throws PulsarRpcClientException {
    ConcurrentHashMap<String, CompletableFuture<V>> pendingRequestsMap = new ConcurrentHashMap<>();
    MessageDispatcherFactory<T, V> dispatcherFactory =
        new MessageDispatcherFactory<>(
            client,
            builder.getRequestSchema(),
            builder.getReplySchema(),
            builder.getReplySubscription());

    Producer<T> producer;
    Consumer<V> consumer;
    RequestSender<T> sender =
        new RequestSender<>(
            null == builder.getReplyTopic()
                ? builder.getReplyTopicsPattern().pattern()
                : builder.getReplyTopic());
    RequestCallBack<V> callBack =
        builder.getCallBack() == null ? new DefaultRequestCallBack<>() : builder.getCallBack();
    ReplyListener<V> replyListener = new ReplyListener<>(pendingRequestsMap, callBack);
    try {
      producer =
          dispatcherFactory.requestProducer(
              builder.getRequestTopic(), builder.getRequestProducerConfig());

      consumer =
          dispatcherFactory.replyConsumer(
              builder.getReplyTopic(),
              replyListener,
              builder.getReplyTopicsPattern(),
              builder.getPatternAutoDiscoveryInterval());
    } catch (IOException e) {
      throw new PulsarRpcClientException(e);
    }

    return new PulsarRpcClientImpl<>(
        pendingRequestsMap, builder.getReplyTimeout(), sender, producer, consumer, callBack);
  }

  @Override
  public void close() throws PulsarRpcClientException {
    try (requestProducer;
        replyConsumer) {
      pendingRequestsMap.forEach(
          (correlationId, future) -> {
            future.cancel(false);
          });
      pendingRequestsMap.clear();
    } catch (PulsarClientException e) {
      throw new PulsarRpcClientException(e);
    }
  }

  @Override
  public V request(String correlationId, T value, Map<String, Object> config)
      throws PulsarRpcClientException {
    try {
      return requestAsync(correlationId, value, config).get();
    } catch (InterruptedException | ExecutionException e) {
      ExceptionHandler.handleInterruptedException(e);
      throw new PulsarRpcClientException(e.getMessage());
    }
  }

  @Override
  public CompletableFuture<V> requestAsync(
      String correlationId, T value, Map<String, Object> config) {
    return internalRequest(correlationId, value, config, -1, -1, null);
  }

  @Override
  public CompletableFuture<V> requestAtAsync(
      String correlationId, T value, Map<String, Object> config, long timestamp) {
    return internalRequest(correlationId, value, config, timestamp, -1, null);
  }

  @Override
  public CompletableFuture<V> requestAfterAsync(
      String correlationId, T value, Map<String, Object> config, long delay, TimeUnit unit) {
    return internalRequest(correlationId, value, config, -1, delay, unit);
  }

  private CompletableFuture<V> internalRequest(
      String correlationId,
      T value,
      Map<String, Object> config,
      long timestamp,
      long delay,
      TimeUnit unit) {
    CompletableFuture<V> replyFuture = new CompletableFuture<>();
    long replyTimeoutMillis = replyTimeout.toMillis();
    TypedMessageBuilder<T> requestMessage = newRequestMessage(correlationId, value, config);
    if (timestamp == -1 && delay == -1) {
      replyFuture
          .orTimeout(replyTimeoutMillis, TimeUnit.MILLISECONDS)
          .exceptionally(
              e -> {
                replyFuture.completeExceptionally(new PulsarRpcClientException(e.getMessage()));
                callback.onTimeout(correlationId, e);
                removeRequest(correlationId);
                return null;
              });
      pendingRequestsMap.put(correlationId, replyFuture);

      sender
          .sendRequest(requestMessage, replyTimeoutMillis)
          .thenAccept(
              requestMessageId -> {
                if (replyFuture.isCancelled() || replyFuture.isCompletedExceptionally()) {
                  removeRequest(correlationId);
                } else {
                  callback.onSendRequestSuccess(correlationId, requestMessageId);
                }
              })
          .exceptionally(
              ex -> {
                if (callback != null) {
                  callback.onSendRequestError(correlationId, ex, replyFuture);
                } else {
                  replyFuture.completeExceptionally(new PulsarRpcClientException(ex.getMessage()));
                }
                removeRequest(correlationId);
                return null;
              });
    } else {
      // Handle Delayed RPC.
      if (pendingRequestsMap.containsKey(correlationId)) {
        removeRequest(correlationId);
      }

      if (timestamp > 0) {
        requestMessage.property(REQUEST_DELIVER_AT_TIME, String.valueOf(timestamp));
        requestMessage.deliverAt(timestamp);
      } else if (delay > 0 && unit != null) {
        String delayedAt = String.valueOf(System.currentTimeMillis() + unit.toMillis(delay));
        requestMessage.property(REQUEST_DELIVER_AT_TIME, delayedAt);
        requestMessage.deliverAfter(delay, unit);
      }
      sender
          .sendRequest(requestMessage, replyTimeoutMillis)
          .thenAccept(
              requestMessageId -> {
                callback.onSendRequestSuccess(correlationId, requestMessageId);
              })
          .exceptionally(
              ex -> {
                if (callback != null) {
                  callback.onSendRequestError(correlationId, ex, replyFuture);
                } else {
                  replyFuture.completeExceptionally(new PulsarRpcClientException(ex.getMessage()));
                }
                return null;
              });
    }

    return replyFuture;
  }

  private TypedMessageBuilder<T> newRequestMessage(
      String correlationId, T value, Map<String, Object> config) {
    TypedMessageBuilder<T> messageBuilder =
        requestProducer.newMessage().key(correlationId).value(value);
    if (!config.isEmpty()) {
      messageBuilder.loadConf(config);
    }
    return messageBuilder;
  }

  public void removeRequest(String correlationId) {
    pendingRequestsMap.remove(correlationId);
  }

  @VisibleForTesting
  public int pendingRequestSize() {
    return pendingRequestsMap.size();
  }
}
