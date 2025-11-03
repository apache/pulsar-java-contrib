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

import static lombok.AccessLevel.PACKAGE;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.Function;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.apache.commons.pool2.impl.GenericKeyedObjectPool;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.rpc.contrib.common.MessageDispatcherFactory;
import org.apache.pulsar.rpc.contrib.common.PulsarRpcServerException;

@RequiredArgsConstructor(access = PACKAGE)
class PulsarRpcServerImpl<T, V> implements PulsarRpcServer<T, V> {
  private final Consumer<?> requestConsumer;
  private final GenericKeyedObjectPool<?, ?> replyProducerPool;

  /**
   * Creates a new instance of {@link PulsarRpcServerImpl} using the specified Pulsar client and
   * configuration. It sets up the necessary consumer and producer resources according to the
   * provided {@link PulsarRpcServerBuilderImpl} settings.
   *
   * @param pulsarClient the Pulsar client to use for connecting to the Pulsar instance
   * @param requestFunction the function to process incoming requests and produce replies
   * @param rollBackFunction the function to handle rollback in case of processing errors
   * @param builder the configuration builder with all necessary settings
   * @return a new instance of {@link PulsarRpcServerImpl}
   * @throws PulsarRpcServerException if there is an error during the server initialization.
   */
  static <T, V> PulsarRpcServerImpl<T, V> create(
      @NonNull PulsarClient pulsarClient,
      @NonNull Function<T, CompletableFuture<V>> requestFunction,
      @NonNull BiConsumer<String, T> rollBackFunction,
      @NonNull PulsarRpcServerBuilderImpl<T, V> builder)
      throws PulsarRpcServerException {
    MessageDispatcherFactory<T, V> dispatcherFactory =
        new MessageDispatcherFactory<>(
            pulsarClient,
            builder.getRequestSchema(),
            builder.getReplySchema(),
            builder.getRequestSubscription());
    ReplyProducerPoolFactory<V> poolFactory = new ReplyProducerPoolFactory<>(dispatcherFactory);
    GenericKeyedObjectPool<String, Producer<V>> pool = new GenericKeyedObjectPool<>(poolFactory);
    ReplySender<T, V> replySender = new ReplySender<>(pool, rollBackFunction);
    RequestListener<T, V> requestListener =
        new RequestListener<>(requestFunction, replySender, rollBackFunction);

    Consumer<T> requestConsumer;
    try {
      requestConsumer =
          dispatcherFactory.requestConsumer(
              builder.getRequestTopic(),
              builder.getPatternAutoDiscoveryInterval(),
              requestListener,
              builder.getRequestTopicsPattern());
    } catch (IOException e) {
      throw new PulsarRpcServerException(e);
    }

    return new PulsarRpcServerImpl<>(requestConsumer, pool);
  }

  @Override
  public void close() throws PulsarRpcServerException {
    try {
      requestConsumer.close();
    } catch (PulsarClientException e) {
      throw new PulsarRpcServerException(e);
    } finally {
      replyProducerPool.close();
    }
  }
}
