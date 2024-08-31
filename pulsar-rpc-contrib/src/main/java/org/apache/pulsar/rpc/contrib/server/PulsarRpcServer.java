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
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.rpc.contrib.common.MessageDispatcherFactory;

@RequiredArgsConstructor(access = PACKAGE)
public class PulsarRpcServer<T, V> implements AutoCloseable {
    private final Consumer<?> requestConsumer;
    private final GenericKeyedObjectPool<?, ?> replyProducerPool;

    public static <T, V> PulsarRpcServerBuilder<T, V> builder(
            @NonNull Schema<T> requestSchema, @NonNull Schema<V> replySchema) {
        return new PulsarRpcServerBuilder<>(requestSchema, replySchema);
    }

    public static <T, V> PulsarRpcServer<T, V> create(
            @NonNull PulsarClient pulsarClient,
            @NonNull Function<T, CompletableFuture<V>> requestFunction,
            @NonNull BiConsumer<String, T> rollBackFunction,
            @NonNull PulsarRpcServerBuilder<T, V> builder) throws IOException {
        MessageDispatcherFactory<T, V> dispatcherFactory = new MessageDispatcherFactory<>(
                pulsarClient,
                builder.getRequestSchema(),
                builder.getReplySchema(),
                builder.getRequestSubscription());
        ReplyProducerPoolFactory<V> poolFactory = new ReplyProducerPoolFactory<>(dispatcherFactory);
        GenericKeyedObjectPool<String, Producer<V>> pool = new GenericKeyedObjectPool<>(poolFactory);
        ReplySender<T, V> replySender = new ReplySender<>(pool, rollBackFunction);
        RequestListener<T, V> requestListener = new RequestListener<>(
                requestFunction,
                replySender,
                rollBackFunction);
        Consumer<T> requestConsumer = dispatcherFactory.requestConsumer(
                builder.getRequestTopic(),
                builder.getPatternAutoDiscoveryInterval(),
                requestListener,
                builder.getRequestTopicsPattern());

        return new PulsarRpcServer<>(
                requestConsumer,
                pool
        );
    }

    @Override
    public void close() throws Exception {
        requestConsumer.close();
        replyProducerPool.close();
    }
}
